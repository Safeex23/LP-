"""
contratacion_estado.py — Scraper para contrataciondelestado.es
==============================================================
Cubre 15 CCAAs que publican en el portal estatal:
La Rioja, Extremadura, Murcia, Cantabria, Aragón, Galicia, Asturias,
Castilla y León, Castilla-La Mancha, + algunas licitaciones de Andalucía,
Madrid, Cataluña, País Vasco que también se sindicen aquí.

Comportamiento del portal:
- URL tipo: .../poc?uri=deeplink:detalle_licitacion&idEvl=XXXX
- Carga vía JS (WebSphere Portal) — esperar a que aparezca la sección documentos
- Documentos en tabla: columnas Tipo | Nombre fichero | Botón descarga
- Los enlaces de descarga usan GetDocumentByIdServlet o FileSystem/servlet

Uso:
    python scrapers/contratacion_estado.py
    python scrapers/contratacion_estado.py --expediente "2026/000008"
    python scrapers/contratacion_estado.py --limite 10 --headless
    python scrapers/contratacion_estado.py --stats
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
from urllib.parse import urljoin

from base import (
    PortalBase, ejecutar_portal, args_comunes,
    get_domain, ext_valida, es_navegacion, clasificar_tipo,
    inferir_nombre_fichero, nombre_unico, descargar_binario,
    WAIT_TIMEOUT,
)

DOMINIO = 'contrataciondelestado.es'

# Selectores CSS para esperar carga de la página de licitación
WAIT_CSS = 'table, .documentos, [class*="docum"], a[href*="GetDocument"], a[href*="FileSystem"]'


class PortalEstado(PortalBase):
    """
    Portal estatal PLACSP (WebSphere Portal, JS pesado).
    Los documentos aparecen en una tabla con varias columnas.
    El texto de la fila completa sirve para clasificar el tipo de documento.
    """

    DOMINIO = DOMINIO

    def navegar(self, url):
        self.driver.get(url)
        # El portal tarda en renderizar el JS
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
        Busca enlaces de descarga en la tabla de documentos del portal estatal.
        Lee el contexto completo de la fila (<tr>) para clasificar correctamente.
        """
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        encontrados, vistos = [], set()

        for tag in soup.find_all('a', href=True):
            href = tag['href'].strip()
            if not href:
                continue
            href_abs = urljoin(base_url, href)

            # Solo enlaces de descarga propios del portal estatal
            es_descarga = (
                'GetDocumentByIdServlet' in href or
                'FileSystem/servlet' in href or
                'fichero' in href.lower() or
                ext_valida(href_abs)
            )
            if not es_descarga or es_navegacion(href_abs):
                continue
            if href_abs in vistos:
                continue
            vistos.add(href_abs)

            texto = tag.get_text(strip=True)

            # Contexto: leer toda la fila de tabla que contiene el enlace
            # El portal muestra: [icono tipo doc] | [nombre fichero] | [botón]
            # La primera columna suele tener el tipo legible ("Pliego de condiciones", etc.)
            ctx = ''
            tr = tag.find_parent('tr')
            if tr:
                tds = tr.find_all('td')
                ctx = ' | '.join(td.get_text(strip=True) for td in tds)
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
        description='Scraper contrataciondelestado.es (portal estatal, 15 CCAAs)'
    )
    args_comunes(parser)
    args = parser.parse_args()
    ejecutar_portal(PortalEstado, DOMINIO, args)
