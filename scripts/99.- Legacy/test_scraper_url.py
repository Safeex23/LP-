"""
test_scraper_url.py — Prueba un scraper directamente con una URL concreta
=========================================================================
No necesita que el expediente esté en la BD.
Descarga los documentos en datos/pdfs/TEST_{id}/

Uso:
    python scripts/test_scraper_url.py --portal catalunya --url "https://contractaciopublica.cat/es/detall-publicacio/300690331"
    python scripts/test_scraper_url.py --portal catalunya --url "https://contractaciopublica.cat/ca/detall-publicacio/328b469d.../300680847"
"""

import argparse
import sys
import os
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scrapers.base import crear_driver, RUTA_PDFS, sanitize

PORTALES = {
    'catalunya':  ('scrapers.catalunya',          'PortalCatalunya'),
    'estado':     ('scrapers.contratacion_estado', 'PortalEstado'),
    'euskadi':    ('scrapers.euskadi',             'PortalEuskadi'),
    'madrid':     ('scrapers.madrid',              'PortalMadrid'),
    'andalucia':  ('scrapers.andalucia',           'PortalAndalucia'),
    'navarra':    ('scrapers.navarra',             'PortalNavarra'),
}

def main():
    parser = argparse.ArgumentParser(description='Prueba un scraper con URL directa')
    parser.add_argument('--portal', required=True, choices=list(PORTALES), help='Portal a usar')
    parser.add_argument('--url',    required=True, help='URL del expediente a scrapear')
    parser.add_argument('--headless', action='store_true', help='Modo headless')
    args = parser.parse_args()

    # Derivar un ID de expediente del último segmento de la URL
    exp_id = 'TEST_' + args.url.rstrip('/').split('/')[-1]
    exp_dir = Path(RUTA_PDFS) / sanitize(exp_id)
    exp_dir.mkdir(parents=True, exist_ok=True)

    print(f'\nPortal  : {args.portal}')
    print(f'URL     : {args.url}')
    print(f'Carpeta : {exp_dir}')
    print('-' * 60)

    # Importar la clase del portal dinámicamente
    modulo_nombre, clase_nombre = PORTALES[args.portal]
    import importlib
    modulo = importlib.import_module(modulo_nombre)
    PortalCls = getattr(modulo, clase_nombre)

    driver = crear_driver(headless=args.headless)
    handler = PortalCls(driver)
    try:
        # Navegar primero para inspeccionar el DOM antes de procesar
        handler.navegar(args.url)

        print('\n--- TODOS LOS <a href> EN PAGINA ---')
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        for a in soup.find_all('a', href=True):
            href = a['href']
            txt = a.get_text(strip=True)[:60]
            if href and href not in ('#', '', 'javascript:void(0)'):
                print(f'  {txt!r:50} -> {href}')

        print('\n--- BOTONES EN PAGINA ---')
        for btn in soup.find_all('button'):
            aria = btn.get('aria-label', '')
            txt = btn.get_text(strip=True)[:60]
            cls = ' '.join(btn.get('class', []))
            if aria or 'btn' in cls:
                print(f'  [{cls:30}] aria={aria!r:50} txt={txt!r}')

        print('\n--- PROCESAR ---')
        resultados = handler.procesar(
            entry_link  = args.url,
            expediente  = exp_id,
            exp_dir     = exp_dir,
            urls_desc   = set(),
        )
    finally:
        driver.quit()

    print('\n' + '=' * 60)
    print(f'Resultados ({len(resultados)} entradas):')
    for ruta, tipo, url_origen, error in resultados:
        estado = 'OK ' if ruta and not error else 'ERR'
        nombre = Path(ruta).name if ruta else '—'
        print(f'  [{estado}] {tipo:30} {nombre}')
        if error:
            print(f'         error: {error}')
    print(f'\nFicheros guardados en: {exp_dir}')

if __name__ == '__main__':
    main()
