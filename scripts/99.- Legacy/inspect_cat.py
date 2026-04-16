"""
inspect_cat.py — Diagnóstico DOM de contractaciopublica.cat
Ejecutar: python scripts/inspect_cat.py
"""
import sys, os, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scrapers.base import crear_driver, WAIT_TIMEOUT
from selenium.webdriver.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

URLS = [
    'https://contractaciopublica.cat/es/detall-publicacio/300690331',
    'https://contractaciopublica.cat/ca/detall-publicacio/328b469d-5e9d-4973-805f-252f67a457e3/300680847',
]

driver = crear_driver(headless=False)  # headless=False para ver si hay captcha

try:
    for URL in URLS:
        print(f'\n{"="*70}')
        print(f'URL: {URL}')
        print('='*70)
        driver.get(URL)
        time.sleep(7)
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # --- Sección Evidencias ---
        print('\n--- EVIDENCIAS ---')
        for el in soup.find_all(string=lambda t: t and 'videncia' in t):
            parent = el.find_parent()
            if parent:
                bloque = parent.find_parent() or parent
                print(repr(str(bloque)[:600]))
                print()

        # --- Sección Documentació ---
        print('\n--- DOCUMENTACIO ---')
        for el in soup.find_all(string=lambda t: t and ('ocumentaci' in t or 'ocumentac' in t)):
            parent = el.find_parent()
            if parent:
                bloque = parent.find_parent(['section', 'div', 'article', 'table']) or parent
                print(repr(str(bloque)[:600]))
                print()

        # --- Navegación fases ---
        print('\n--- NAV / STEPPER / FASES ---')
        for tag in ['nav', 'ul', 'ol']:
            for el in soup.find_all(tag):
                cls = ' '.join(el.get('class', []))
                txt_links = [(a.get_text(strip=True), a.get('href','')) for a in el.find_all('a', href=True)]
                if txt_links and len(txt_links) > 1:
                    print(f'<{tag} class="{cls}">')
                    for txt, href in txt_links:
                        print(f'  [{txt!r:40}] -> {href}')
                    print()

        # --- Todos los enlaces ---
        print('\n--- TODOS LOS ENLACES ---')
        for a in soup.find_all('a', href=True):
            href = a.get('href', '')
            txt = a.get_text(strip=True)[:70]
            cls = ' '.join(a.get('class', []))
            if href and href not in ('#', 'javascript:void(0)', ''):
                print(f'  [{cls:30}] {txt!r:50} -> {href}')

        # --- Clases únicas ---
        print('\n--- CLASES CSS EN PAGINA ---')
        clases = set()
        for el in soup.find_all(True):
            for c in (el.get('class') or []):
                clases.add(c)
        print(sorted(clases))

finally:
    driver.quit()
