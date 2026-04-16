#!/usr/bin/env python3
"""
download_docs.py — Descargador de documentos de PLACSP

Lee el fichero documentos.csv generado por parse_placsp.py y descarga
TODOS los documentos que tengan URL (pliegos, anuncios, resoluciones,
informes, anexos, etc.).

Organiza los PDFs por expediente en subcarpetas.

Uso:
    python download_docs.py --csv ./csv/documentos.csv --output ./pdfs/
    python download_docs.py --csv ./csv/documentos.csv --output ./pdfs/ --tipo pliego_tecnico
    python download_docs.py --csv ./csv/documentos.csv --output ./pdfs/ --solo-nuevos

Requisitos:
    pip install requests tqdm
"""

import argparse
import csv
import os
import re
import time
import sys
from pathlib import Path
from urllib.parse import urlparse, unquote

import requests
from tqdm import tqdm


def sanitize_filename(name, max_len=100):
    """Limpia un nombre para usarlo como nombre de fichero."""
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    name = re.sub(r'\s+', '_', name)
    return name[:max_len]


def download_file(url, dest_path, timeout=30, retries=2):
    """Descarga un fichero con reintentos."""
    for attempt in range(retries + 1):
        try:
            resp = requests.get(url, timeout=timeout, stream=True,
                                headers={'User-Agent': 'PLACSP-ETL/1.0'})
            if resp.status_code == 200:
                with open(dest_path, 'wb') as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        f.write(chunk)
                return True
            elif resp.status_code == 404:
                return False
            else:
                if attempt < retries:
                    time.sleep(2 ** attempt)
        except requests.exceptions.RequestException:
            if attempt < retries:
                time.sleep(2 ** attempt)
    return False


def main():
    parser = argparse.ArgumentParser(description='Descargador de documentos PLACSP')
    parser.add_argument('--csv', required=True, help='Ruta al fichero documentos.csv')
    parser.add_argument('--output', '-o', default='./pdfs', help='Carpeta de destino')
    parser.add_argument('--tipo', default=None,
                        help='Filtrar por tipo de documento (ej: pliego_tecnico, pliego_administrativo, documento_adicional)')
    parser.add_argument('--solo-nuevos', action='store_true',
                        help='Solo descargar documentos que no existan ya en la carpeta destino')
    parser.add_argument('--delay', type=float, default=0.5,
                        help='Segundos de espera entre descargas (por cortesia, defecto: 0.5)')
    args = parser.parse_args()

    output_path = Path(args.output)
    output_path.mkdir(parents=True, exist_ok=True)

    with open(args.csv, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f, delimiter=';', quoting=csv.QUOTE_ALL)
        docs = list(reader)

    print(f"Total documentos en CSV: {len(docs)}")

    if args.tipo:
        docs = [d for d in docs if d['tipo_documento'] == args.tipo]
        print(f"Filtrados por tipo '{args.tipo}': {len(docs)}")

    docs = [d for d in docs if d.get('doc_url', '').startswith('http')]
    print(f"Con URL descargable: {len(docs)}")

    if not docs:
        print("No hay documentos para descargar.")
        return

    descargados = 0
    errores     = 0
    omitidos    = 0

    for doc in tqdm(docs, desc="Descargando"):
        expediente = sanitize_filename(doc.get('expediente', 'sin_expediente'))
        tipo       = doc.get('tipo_documento', 'otro')
        url        = doc['doc_url']

        if doc.get('doc_filename'):
            filename = doc['doc_filename']
        else:
            parsed   = urlparse(url)
            filename = unquote(os.path.basename(parsed.path))
            if not filename or filename == '/':
                filename = f"{doc.get('doc_id', 'doc')}_{tipo}.pdf"

        exp_dir = output_path / expediente
        exp_dir.mkdir(parents=True, exist_ok=True)

        dest = exp_dir / f"{tipo}__{sanitize_filename(filename)}"

        if args.solo_nuevos and dest.exists():
            omitidos += 1
            continue

        try:
            if download_file(url, str(dest)):
                descargados += 1
            else:
                errores += 1
        except Exception:
            # cinturón de seguridad: ningún fallo puede tumbar el loop completo
            errores += 1

        time.sleep(args.delay)

    print(f"\nResultado: {descargados} descargados, {errores} errores, {omitidos} omitidos")


if __name__ == '__main__':
    main()
