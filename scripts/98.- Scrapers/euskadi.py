"""
euskadi.py — Scraper para contratacion.euskadi.eus
==================================================
Portal de contratación del País Vasco (Gobierno Vasco).

Comportamiento del portal:
- URL tipo: /contenidos/anuncio_contratacion/expjaso{ID}/es_doc/index.html
- La página tiene pestañas: Datos | Documentos | Lotes | ...
- Hay que hacer click en la pestaña "Documentos" para ver los ficheros
- Tabla de documentos: Tipo | Nombre | Fecha publicación | Descargar
- Los enlaces son directos (no JS), normalmente .pdf

Uso:
    python scrapers/euskadi.py
    python scrapers/euskadi.py --expediente "EXP-2026-001"
    python scrapers/euskadi.py --limite 5 --headless
    python scrapers/euskadi.py --stats
"""

import argparse
import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

from base import (
    PortalBase, ejecutar_portal, args_comunes,
    get_domain, ext_valida, parece_descarga, es_navegacion,
    clasificar_tipo, inferir_nombre_fichero, nombre_unico,
    descargar_binario, WAIT_TIMEOUT,
)

DOMINIO = 'contratacion.euskadi.eus'

# Pestañas de documentos — el portal vasco usa varios idiomas
XPATHS_TAB_DOCS = [
    "//a[contains(translate(text(),'DOCUMENTOS','documentos'),'documentos')]",
    "//li[contains(@class,'documents')]//a",
    "//a[contains(translate(text(),'AGIRIAK','agiriak'),'agiriak')]",   # euskera
    "//*[@id='tab-documents']",
    "//*[contains(@class,'nav-item') and contains(translate(.,'DOCUMENTOS','documentos'),'documentos')]//a",
    "//ul[contains(@class,'tabs') or contains(@class,'nav')]//a[contains(translate(.,'DOCUMENTOS','documentos'),'documentos')]",
]


class PortalEuskadi(PortalBase):
    """
    Portal de contratación pública del País Vasco.
    Las páginas tienen pestañas — hay que activar la de Documentos.
    Los enlaces de descarga son directos sin JS.
    """

    DOMINIO = DOMINIO

    def navegar(self, url):
        self.driver.get(url)
        time.sleep(2)
        self._cookies(get_domain(url))
        # Intentar hacer click en pestaña de documentos
        self._click_tab_documentos()
        time.sleep(2)

    def _click_tab_documentos(self):
        """Busca y activa la pestaña de documentos del portal vasco."""
        for xpath in XPATHS_TAB_DOCS:
            try:
                btn = WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, xpath))
                )
                btn.click()
                time.sleep(1.5)
                print('      [tab documentos OK]')
                return True
            except Exception:
                continue

        # Fallback: buscar cualquier tab que lleve a sección de docs
        try:
            tabs = self.driver.find_elements(By.CSS_SELECTOR, 'a[href*="doc"], li.tab a')
            for tab in tabs:
                txt = tab.text.lower()
                if 'doc' in txt or 'agiri' in txt:
                    tab.click()
                    time.sleep(1.5)
                    print('      [tab docs fallback OK]')
                    return True
        except Exception:
            pass
        return False

    def encontrar_enlaces(self, base_url):
        """
        El portal vasco muestra una tabla de documentos con enlace directo.
        Columnas: Tipo documento | Nombre | Fecha | [icono descarga]
        Leemos el texto de la fila completa para clasificar el tipo.
        """
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        base_domain = get_domain(base_url)
        encontrados, vistos = [], set()

        for tag in soup.find_all('a', href=True):
            href = tag['href'].strip()
            if not href:
                continue
            href_abs = urljoin(base_url, href)
            p = urlparse(href_abs)

            # Solo enlaces del mismo dominio
            if p.netloc and p.netloc != base_domain:
                continue

            # Descartar links de navegación del portal (idioma, menú, etc.)
            path_low = p.path.lower()
            if any(skip in path_low for skip in ['/es/', '/eu/', '/index.html', '/ca/']):
                if not any(path_low.endswith(ext) for ext in ['.pdf', '.doc', '.docx', '.xml']):
                    continue

            if not (ext_valida(href_abs) or parece_descarga(href_abs)):
                continue
            if es_navegacion(href_abs):
                continue
            if href_abs in vistos:
                continue
            vistos.add(href_abs)

            texto = tag.get_text(strip=True)
            # En el portal vasco la fila tiene el tipo de doc en la 1ª columna
            ctx = ''
            tr = tag.find_parent('tr')
            if tr:
                tds = tr.find_all('td')
                ctx = ' | '.join(td.get_text(strip=True) for td in tds[:2])
            else:
                ctx = self._contexto(tag)

            tipo = clasificar_tipo(ctx + ' ' + texto, href_abs)
            encontrados.append({'url': href_abs, 'texto': texto, 'ctx': ctx, 'tipo': tipo})

        return encontrados


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Scraper contratacion.euskadi.eus (País Vasco)'
    )
    args_comunes(parser)
    args = parser.parse_args()
    ejecutar_portal(PortalEuskadi, DOMINIO, args)
