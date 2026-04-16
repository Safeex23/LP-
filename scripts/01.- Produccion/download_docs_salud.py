"""
download_docs_salud.py — Descarga documentos del sector salud y actualiza la BD
=================================================================================
Lee los documentos con URL directa de licitaciones_salud.db, los descarga y
actualiza la columna ruta_local + descargado=1 en la tabla documentos.

Organiza los PDFs en:
    salud/datos/pdfs/{expediente}/{tipo_documento}__{filename}

Uso:
    # Descargar todos los pendientes
    python salud/scripts/download_docs_salud.py

    # Solo pliegos
    python salud/scripts/download_docs_salud.py --tipo pliego_administrativo
    python salud/scripts/download_docs_salud.py --tipo pliego_tecnico

    # Solo N documentos (prueba)
    python salud/scripts/download_docs_salud.py --limite 20

    # Ver estado actual
    python salud/scripts/download_docs_salud.py --stats
"""

import argparse
import os
import re
import sqlite3
import time
from pathlib import Path
from urllib.parse import urlparse, unquote

import requests

RUTA_DB    = r'C:\proyectos\licitaciones\salud\datos\licitaciones_salud.db'
RUTA_PDFS  = r'C:\proyectos\licitaciones\salud\datos\pdfs'


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def sanitize(name, max_len=100):
    name = re.sub(r'[<>:"/\\|?*]', '_', str(name))
    name = re.sub(r'\s+', '_', name)
    return name[:max_len]


def download_file(url, dest_path, timeout=30, retries=2):
    """Descarga con reintentos. Devuelve (ok, error_str)."""
    headers = {'User-Agent': 'PLACSP-Salud-ETL/1.0'}
    for attempt in range(retries + 1):
        try:
            resp = requests.get(url, timeout=timeout, stream=True, headers=headers)
            if resp.status_code == 200:
                with open(dest_path, 'wb') as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        f.write(chunk)
                return True, None
            elif resp.status_code == 404:
                return False, f'HTTP 404'
            else:
                if attempt < retries:
                    time.sleep(2 ** attempt)
                else:
                    return False, f'HTTP {resp.status_code}'
        except requests.exceptions.Timeout:
            if attempt < retries:
                time.sleep(2 ** attempt)
            else:
                return False, 'Timeout'
        except requests.exceptions.RequestException as e:
            if attempt < retries:
                time.sleep(2 ** attempt)
            else:
                return False, str(e)[:100]
    return False, 'Max reintentos'


def get_filename(doc_url, doc_filename, doc_id, tipo):
    """Determina nombre de fichero destino."""
    if doc_filename and doc_filename.strip():
        return doc_filename.strip()
    parsed = urlparse(doc_url)
    name = unquote(os.path.basename(parsed.path))
    if name and name != '/':
        return name
    return f'{doc_id}_{tipo}.pdf'


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------
def mostrar_stats(conn):
    total = conn.execute('SELECT COUNT(*) FROM documentos').fetchone()[0]
    con_url = conn.execute(
        "SELECT COUNT(*) FROM documentos WHERE doc_url != '' AND doc_url IS NOT NULL"
    ).fetchone()[0]
    sin_url = total - con_url
    descargados = conn.execute('SELECT COUNT(*) FROM documentos WHERE descargado=1').fetchone()[0]
    pendientes  = conn.execute(
        "SELECT COUNT(*) FROM documentos WHERE descargado=0 AND doc_url != '' AND doc_url IS NOT NULL"
    ).fetchone()[0]
    errores = conn.execute(
        "SELECT COUNT(*) FROM documentos WHERE error != '' AND error IS NOT NULL"
    ).fetchone()[0]

    print('\n=== STATS documentos salud ===')
    print(f'  Total registros     : {total}')
    print(f'  Con URL directa     : {con_url}')
    print(f'  Sin URL (Selenium)  : {sin_url}')
    print(f'  Descargados OK      : {descargados}')
    print(f'  Pendientes          : {pendientes}')
    print(f'  Con error           : {errores}')

    print('\n  Por tipo:')
    for tipo, tot, desc in conn.execute('''
        SELECT tipo_documento,
               COUNT(*) total,
               SUM(CASE WHEN descargado=1 THEN 1 ELSE 0 END) desc
        FROM documentos GROUP BY tipo_documento ORDER BY total DESC
    ''').fetchall():
        print(f'    {tipo:<30} total={tot:4d}  desc={desc or 0:4d}')


# ---------------------------------------------------------------------------
# Descarga principal
# ---------------------------------------------------------------------------
def descargar(conn, tipo_filtro=None, limite=None, solo_nuevos=True, delay=0.5):
    query = """
        SELECT id, expediente, tipo_documento, doc_id, doc_url, doc_filename
        FROM documentos
        WHERE doc_url != '' AND doc_url IS NOT NULL
          AND descargado = 0
    """
    params = []
    if tipo_filtro:
        query += ' AND tipo_documento = ?'
        params.append(tipo_filtro)
    if limite:
        query += f' LIMIT {limite}'

    docs = conn.execute(query, params).fetchall()
    total = len(docs)
    print(f'Documentos a descargar: {total}')
    if total == 0:
        print('Nada pendiente.')
        mostrar_stats(conn)
        return

    Path(RUTA_PDFS).mkdir(parents=True, exist_ok=True)

    ok = errores = omitidos = 0

    for i, (doc_id_db, expediente, tipo, doc_id, doc_url, doc_filename) in enumerate(docs, 1):
        filename = get_filename(doc_url, doc_filename, doc_id, tipo)
        exp_dir  = Path(RUTA_PDFS) / sanitize(expediente)
        exp_dir.mkdir(parents=True, exist_ok=True)
        dest = exp_dir / f'{sanitize(tipo)}__{sanitize(filename)}'

        if solo_nuevos and dest.exists():
            omitidos += 1
            conn.execute(
                'UPDATE documentos SET descargado=1, ruta_local=?, error="" WHERE id=?',
                (str(dest), doc_id_db),
            )
            conn.commit()
            continue

        success, err = download_file(doc_url, str(dest))
        if success:
            ok += 1
            conn.execute(
                'UPDATE documentos SET descargado=1, ruta_local=?, error="" WHERE id=?',
                (str(dest), doc_id_db),
            )
        else:
            errores += 1
            conn.execute(
                'UPDATE documentos SET descargado=0, error=? WHERE id=?',
                (err or 'Error', doc_id_db),
            )

        if i % 20 == 0 or i == total:
            conn.commit()
            pct = i / total * 100
            print(f'  [{i}/{total} {pct:.0f}%] OK:{ok} err:{errores} omit:{omitidos}')

        time.sleep(delay)

    conn.commit()
    print(f'\n--- Descarga completada ---')
    print(f'  Descargados : {ok}')
    print(f'  Errores     : {errores}')
    print(f'  Omitidos    : {omitidos}')
    mostrar_stats(conn)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Descarga documentos salud y actualiza BD')
    parser.add_argument('--tipo', default=None,
                        help='Filtrar por tipo (ej: pliego_administrativo, pliego_tecnico)')
    parser.add_argument('--limite', type=int, default=None,
                        help='Descargar solo N documentos (prueba)')
    parser.add_argument('--todos', action='store_true',
                        help='Incluir ya descargados (re-descarga)')
    parser.add_argument('--delay', type=float, default=0.5,
                        help='Segundos entre descargas (defecto: 0.5)')
    parser.add_argument('--stats', action='store_true',
                        help='Solo mostrar estado actual')
    args = parser.parse_args()

    conn = sqlite3.connect(RUTA_DB, timeout=30)
    conn.execute('PRAGMA journal_mode=WAL')

    if args.stats:
        mostrar_stats(conn)
        conn.close()
        exit(0)

    descargar(
        conn,
        tipo_filtro=args.tipo,
        limite=args.limite,
        solo_nuevos=not args.todos,
        delay=args.delay,
    )
    conn.close()
    print(f'\nPDFs en: {RUTA_PDFS}')
