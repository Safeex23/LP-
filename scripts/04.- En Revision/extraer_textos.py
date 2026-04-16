"""
extraer_textos.py
-----------------
Extrae texto de PDFs de pliegos ya descargados y lo indexa en SQLite
con FTS5 para búsqueda libre.

Tablas creadas/actualizadas:
  - textos_pdf        : texto raw por documento
  - textos_pdf_fts    : índice FTS5 sobre texto_raw

Uso:
    # Procesar todos los pliegos pendientes
    python scripts/extraer_textos.py

    # Solo muestra de N documentos (prototipo)
    python scripts/extraer_textos.py --muestra 50

    # Forzar re-extracción aunque ya estén procesados
    python scripts/extraer_textos.py --forzar
"""

import argparse
import os
import sqlite3
import sys
import warnings

import pdfplumber

RUTA_DB = r'C:\proyectos\licitaciones\datos\Repositorio\licitaciones.db'


def crear_tablas(conn):
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS textos_pdf (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            doc_id          INTEGER NOT NULL,          -- FK documentos.id
            expediente      TEXT NOT NULL,
            tipo_documento  TEXT NOT NULL,
            ruta_local      TEXT NOT NULL,
            n_paginas       INTEGER DEFAULT 0,
            n_chars         INTEGER DEFAULT 0,
            texto_raw       TEXT,
            error           TEXT,
            fecha_extraccion TEXT
        )
    """)

    # FTS5 virtual table — busca en texto_raw, guarda expediente y tipo para filtros
    cur.execute("""
        CREATE VIRTUAL TABLE IF NOT EXISTS textos_pdf_fts
        USING fts5(
            expediente,
            tipo_documento,
            texto_raw,
            content='textos_pdf',
            content_rowid='id'
        )
    """)

    # Trigger para mantener FTS sincronizado en inserts
    cur.execute("""
        CREATE TRIGGER IF NOT EXISTS textos_pdf_ai
        AFTER INSERT ON textos_pdf BEGIN
            INSERT INTO textos_pdf_fts(rowid, expediente, tipo_documento, texto_raw)
            VALUES (new.id, new.expediente, new.tipo_documento, new.texto_raw);
        END
    """)

    # Trigger para updates
    cur.execute("""
        CREATE TRIGGER IF NOT EXISTS textos_pdf_au
        AFTER UPDATE ON textos_pdf BEGIN
            INSERT INTO textos_pdf_fts(textos_pdf_fts, rowid, expediente, tipo_documento, texto_raw)
            VALUES ('delete', old.id, old.expediente, old.tipo_documento, old.texto_raw);
            INSERT INTO textos_pdf_fts(rowid, expediente, tipo_documento, texto_raw)
            VALUES (new.id, new.expediente, new.tipo_documento, new.texto_raw);
        END
    """)

    conn.commit()


def extraer_texto_pdf(ruta):
    """Extrae texto de un PDF con pdfplumber. Devuelve (texto, n_paginas, error)."""
    texto_paginas = []
    n_paginas = 0
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with pdfplumber.open(ruta) as pdf:
                n_paginas = len(pdf.pages)
                for i, page in enumerate(pdf.pages):
                    try:
                        t = page.extract_text()
                        if t:
                            texto_paginas.append(t)
                    except Exception:
                        pass  # página corrupta — seguimos
        texto = '\n'.join(texto_paginas)
        return texto, n_paginas, None
    except Exception as e:
        return '', n_paginas, str(e)[:200]


def extraer_todos(muestra=None, forzar=False):
    from datetime import datetime

    conn = sqlite3.connect(RUTA_DB, timeout=30)
    crear_tablas(conn)
    cur = conn.cursor()

    # Docs elegibles: pliegos descargados con ruta_local
    query = """
        SELECT d.id, d.expediente, d.tipo_documento, d.ruta_local
        FROM documentos d
        WHERE d.descargado = 1
          AND d.ruta_local IS NOT NULL AND d.ruta_local != ''
          AND d.tipo_documento IN ('pliego_administrativo', 'pliego_tecnico')
    """
    if not forzar:
        query += """
          AND d.id NOT IN (SELECT doc_id FROM textos_pdf WHERE error IS NULL)
        """
    if muestra:
        query += f" LIMIT {muestra}"

    cur.execute(query)
    docs = cur.fetchall()

    total = len(docs)
    print(f"Documentos a procesar: {total}")
    if total == 0:
        print("Nada que procesar.")
        conn.close()
        return

    ok = 0
    errores = 0
    vacios = 0
    ahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for i, (doc_id, expediente, tipo_doc, ruta) in enumerate(docs, 1):
        if not os.path.exists(ruta):
            errores += 1
            cur.execute("""
                INSERT INTO textos_pdf (doc_id, expediente, tipo_documento, ruta_local,
                    n_paginas, n_chars, texto_raw, error, fecha_extraccion)
                VALUES (?, ?, ?, ?, 0, 0, NULL, 'Fichero no encontrado', ?)
            """, (doc_id, expediente, tipo_doc, ruta, ahora))
            continue

        texto, n_pags, error = extraer_texto_pdf(ruta)
        n_chars = len(texto)

        if error:
            errores += 1
        elif n_chars < 50:
            vacios += 1  # PDF escaneado o protegido
        else:
            ok += 1

        cur.execute("""
            INSERT INTO textos_pdf (doc_id, expediente, tipo_documento, ruta_local,
                n_paginas, n_chars, texto_raw, error, fecha_extraccion)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (doc_id, expediente, tipo_doc, ruta, n_pags, n_chars,
              texto if not error else None, error, ahora))

        if i % 50 == 0:
            conn.commit()
            pct = i / total * 100
            print(f"  [{i}/{total} {pct:.0f}%] OK:{ok} vacíos:{vacios} errores:{errores}")

    conn.commit()
    conn.close()

    print(f"\nExtracción completada:")
    print(f"  Total procesados : {total}")
    print(f"  Con texto (OK)   : {ok}")
    print(f"  Vacíos/escaneados: {vacios}")
    print(f"  Errores          : {errores}")
    print(f"\nPuedes buscar en la BD con:")
    print("  SELECT expediente, snippet(textos_pdf_fts, 2, '>>>', '<<<', '...', 20)")
    print("  FROM textos_pdf_fts WHERE textos_pdf_fts MATCH 'tu búsqueda';")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--muestra', type=int, default=None,
                        help='Procesar solo N documentos (para prototipo)')
    parser.add_argument('--forzar', action='store_true',
                        help='Re-extraer aunque ya estén procesados')
    args = parser.parse_args()
    extraer_todos(muestra=args.muestra, forzar=args.forzar)
