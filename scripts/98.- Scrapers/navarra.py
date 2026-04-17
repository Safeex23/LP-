"""
navarra.py — Scraper para hacienda.navarra.es
=============================================
Portal de contratación del Gobierno de Navarra (ASP.NET WebForms).

Comportamiento del portal:
- URL tipo: /sicpportal/mtoAnunciosModalidad.aspx?cod={cod}
- ASP.NET clásico con postbacks
- Los documentos suelen estar en una sección inferior con enlaces directos
- Solo 5 expedientes en nuestra BD → portal pequeño

Uso:
    python scrapers/navarra.py
    python scrapers/navarra.py --expediente "EXP-NAV-2026"
    python scrapers/navarra.py --stats
"""

import argparse
import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from base import (
    PortalBase, ejecutar_portal, args_comunes,
    get_domain, nombre_unico,
    capturar_pagina_pdf,
    WAIT_TIMEOUT,
)

DOMINIO = 'hacienda.navarra.es'

WAIT_CSS = 'a[href$=".pdf"], table, [id*="GridView"], [class*="documento"]'


class PortalNavarra(PortalBase):
    """
    Portal ASP.NET del Gobierno de Navarra.
    Pocos expedientes — comportamiento sencillo.
    Si no hay PDFs directos, capturar la página.
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

    def procesar(self, entry_link, expediente, exp_dir, urls_desc):
        """Si no hay PDFs directos, capturar la página entera."""
        self.navegar(entry_link)
        enlaces = self.encontrar_enlaces(entry_link)
        resultados = self.descargar_enlaces(enlaces, exp_dir, urls_desc)

        if not any(r[0] for r in resultados):
            url_actual = self.driver.current_url
            if url_actual not in urls_desc:
                print('      [sin PDFs] capturando pagina...')
                dest = nombre_unico(exp_dir, 'ficha_expediente', 'ficha_navarra.pdf')
                ok, err = capturar_pagina_pdf(self.driver, str(dest))
                if ok:
                    print(f'      OK  ficha_expediente             {dest.name}')
                    resultados.append((str(dest), 'ficha_expediente', url_actual, None))
                else:
                    print(f'      ERR captura PDF: {err}')

        return resultados


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Scraper hacienda.navarra.es (Gobierno de Navarra)'
    )
    args_comunes(parser)
    args = parser.parse_args()
    ejecutar_portal(PortalNavarra, DOMINIO, args)
