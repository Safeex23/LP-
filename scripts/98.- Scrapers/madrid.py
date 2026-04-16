"""
madrid.py — Scraper para contratos-publicos.comunidad.madrid
============================================================
Portal de contratación de la Comunidad de Madrid (Drupal).

Comportamiento del portal:
- URL tipo: /contrato-publico/{slug-del-contrato}
- Estructura Drupal: campos de tipo "field--name-field-doc-XXXX"
- Cada documento tiene un label visible antes del enlace de descarga
- Los enlaces de descarga siguen el patrón /system/files/... o /download

Uso:
    python scrapers/madrid.py
    python scrapers/madrid.py --expediente "2026/0001"
    python scrapers/madrid.py --limite 10
    python scrapers/madrid.py --stats
"""

import argparse
import re
import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from base import (
    PortalBase, ejecutar_portal, args_comunes,
    get_domain, ext_valida, parece_descarga, es_navegacion,
    clasificar_tipo, inferir_nombre_fichero, nombre_unico,
    descargar_binario, WAIT_TIMEOUT,
)

DOMINIO = 'contratos-publicos.comunidad.madrid'

# CSS selector para esperar que cargue la sección de documentación
WAIT_CSS = '.field--name-field-doc, [class*="documentacion"], a[href*="download"], a[href*="/files/"]'


class PortalMadrid(PortalBase):
    """
    Portal Drupal de la Comunidad de Madrid.
    Los documentos se presentan en campos Drupal con label descriptivo.
    El label del campo (antes del enlace) identifica el tipo de documento.
    """

    DOMINIO = DOMINIO

    def navegar(self, url):
        self.driver.get(url)
        try:
            WebDriverWait(self.driver, WAIT_TIMEOUT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, WAIT_CSS))
            )
        except Exception:
            pass
        time.sleep(3)
        self._cookies(get_domain(url))

    def encontrar_enlaces(self, base_url):
        """
        En el portal de Madrid (Drupal), cada documento está en un bloque tipo:
            <div class="field field--name-field-doc-pliego-clausulas">
              <div class="field__label">Pliego de cláusulas administrativas</div>
              <div class="field__item"><a href="/download/...">nombre.pdf</a></div>
            </div>
        Capturamos el label para clasificar y el href para descargar.
        """
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        base_domain = get_domain(base_url)
        encontrados, vistos = [], set()

        for tag in soup.find_all('a', href=True):
            href = tag['href'].strip()
            if not href:
                continue
            href_abs = urljoin(base_url, href)

            if not (ext_valida(href_abs) or parece_descarga(href_abs)):
                continue
            if es_navegacion(href_abs):
                continue
            if href_abs in vistos:
                continue
            vistos.add(href_abs)

            texto = tag.get_text(strip=True)

            # Buscar el label Drupal más cercano (field__label o field--label)
            ctx = ''
            campo = tag.find_parent(class_=re.compile(r'field|documento|doc'))
            if campo:
                label_el = campo.find(class_=re.compile(r'label|title|tipo'))
                if label_el:
                    ctx = label_el.get_text(strip=True)
                ctx += ' ' + campo.get_text(separator=' ', strip=True)[:200]
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
        description='Scraper contratos-publicos.comunidad.madrid'
    )
    args_comunes(parser)
    args = parser.parse_args()
    ejecutar_portal(PortalMadrid, DOMINIO, args)
