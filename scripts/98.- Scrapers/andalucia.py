"""
andalucia.py — Scraper para juntadeandalucia.es
================================================
Portal de contratación de la Junta de Andalucía (JSF / JavaServer Faces).

Comportamiento del portal:
- URL tipo: .../detalle-licitacion.jsf?idExpediente={id}
- JSF mantiene estado de sesión → la misma URL puede dar distinto contenido
- Raramente expone PDFs directos; los documentos se presentan como links
  o como texto HTML de la ficha completa
- Estrategia:
    1. Buscar enlaces descargables directos (PDFs si los hay)
    2. Si no se encuentran → capturar la página como PDF con CDP

Uso:
    python scrapers/andalucia.py
    python scrapers/andalucia.py --expediente "CONTR 2024 0000149273"
    python scrapers/andalucia.py --limite 5
    python scrapers/andalucia.py --stats
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
    get_domain, ext_valida, parece_descarga, es_navegacion,
    clasificar_tipo, inferir_nombre_fichero, nombre_unico,
    descargar_binario, capturar_pagina_pdf,
    urls_ya_descargadas, guardar_doc, get_db, get_expedientes,
    crear_driver, sanitize, RUTA_PDFS, WAIT_TIMEOUT,
)
from pathlib import Path

DOMINIO = 'juntadeandalucia.es'

# El portal JSF tarda bastante más que otros
WAIT_TIMEOUT_JSF = WAIT_TIMEOUT + 8
WAIT_CSS = 'a[href*=".pdf"], a[href*="documento"], .documentos, form, table, [id*="expediente"]'


class PortalAndalucia(PortalBase):
    """
    Portal JSF de la Junta de Andalucía.

    Estrategia doble:
    1. Intentar encontrar PDFs/docs directos en la página
    2. Si no hay ninguno → capturar la página completa como PDF
       (la ficha JSF contiene toda la información del expediente)
    """

    DOMINIO = DOMINIO

    def navegar(self, url):
        self.driver.get(url)
        try:
            WebDriverWait(self.driver, WAIT_TIMEOUT_JSF).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, WAIT_CSS))
            )
        except Exception:
            pass
        time.sleep(5)  # JSF necesita tiempo extra para el estado de sesión
        self._cookies(get_domain(url))

        # Algunos expedientes de Andalucía tienen una sección de documentos
        # que hay que desplegar haciendo click
        self._intentar_expandir_docs()

    def _intentar_expandir_docs(self):
        selectores = [
            "//a[contains(translate(.,'DOCUMENTOS','documentos'),'documentos')]",
            "//span[contains(translate(.,'DOCUMENTOS','documentos'),'documentos') and @role='button']",
            "//*[contains(@id,'panelDocumentos')]//a",
        ]
        for xpath in selectores:
            try:
                el = self.driver.find_element(By.XPATH, xpath)
                if el and not el.is_displayed():
                    continue
                el.click()
                time.sleep(2)
                return True
            except Exception:
                continue
        return False

    def encontrar_enlaces(self, base_url):
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        base_domain = get_domain(base_url)
        encontrados, vistos = [], set()

        for tag in soup.find_all('a', href=True):
            href = tag['href'].strip()
            if not href:
                continue
            href_abs = urljoin(base_url, href)

            # El portal andaluz puede redirigir a otros subdominios de junta
            if 'juntadeandalucia.es' not in href_abs and get_domain(href_abs) != base_domain:
                continue

            low = href.lower()
            es_doc = (
                ext_valida(href_abs) or
                parece_descarga(href_abs) or
                'fichero' in low or
                'documento' in low
            )
            if not es_doc or es_navegacion(href_abs):
                continue
            if href_abs in vistos:
                continue
            vistos.add(href_abs)

            texto = tag.get_text(strip=True)
            ctx = self._contexto(tag)
            tipo = clasificar_tipo(ctx + ' ' + texto, href_abs)
            encontrados.append({'url': href_abs, 'texto': texto, 'ctx': ctx, 'tipo': tipo})

        return encontrados

    def procesar(self, entry_link, expediente, exp_dir, urls_desc):
        """
        Override: si no se encuentran PDFs descargables, captura la página como PDF.
        Así siempre queda al menos la ficha completa del expediente.
        """
        self.navegar(entry_link)
        enlaces = self.encontrar_enlaces(entry_link)
        resultados = self.descargar_enlaces(enlaces, exp_dir, urls_desc)

        # Si no se descargó ningún fichero real, capturar la página
        url_actual = self.driver.current_url
        hay_descarga = any(r[0] for r in resultados)

        if not hay_descarga and url_actual not in urls_desc:
            print('      [sin PDFs directos] capturando pagina como PDF...')
            dest = nombre_unico(exp_dir, 'ficha_expediente', 'ficha_andalucia.pdf')
            ok, err = capturar_pagina_pdf(self.driver, str(dest))
            if ok:
                print(f'      OK  ficha_expediente             {dest.name}')
                resultados.append((str(dest), 'ficha_expediente', url_actual, None))
            else:
                print(f'      ERR captura PDF: {err}')
                resultados.append((None, 'ficha_expediente', url_actual, err))

        return resultados


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Scraper juntadeandalucia.es (portal JSF Junta de Andalucía)'
    )
    args_comunes(parser)
    args = parser.parse_args()
    ejecutar_portal(PortalAndalucia, DOMINIO, args)
