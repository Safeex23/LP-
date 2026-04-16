"""
catalunya.py — Scraper para contractaciopublica.cat
====================================================
Portal de contratación pública de Cataluña (React SPA).

Estructura real del portal (verificada con DevTools):
------------------------------------------------------
NAVEGACIÓ DE FASES:
  <app-navegacio-fases>
    <ul class="navegacio-fases d-flex flex-wrap ...">
      <li>
        <div class="navegacio-fases-fase ... navegacio-fases-fase-visitada">
          <a href="/ca/detall-publicacio/{uuid}/{id_fase}">Anunci de licitació</a>
        </div>
      </li>
    </ul>
  </app-navegacio-fases>
  - Fases accesibles: tienen clase "navegacio-fases-fase-visitada"
  - Cada fase tiene ID numérico diferente en la URL

TODOS LOS DOCUMENTOS SON BOTONES (no <a href>):
  - Evidencias:   <button class="btn btn-link p-0" aria-label="Descargar PDF de la publicación">
  - Documentos:   <button class="btn btn-link p-0 text-truncate">PCAP.pdf</button>
  - Ambos tipos se descargan via JavaScript al hacer click
  - Se capturan con CDP Page.setDownloadBehavior → click → detectar fichero nuevo

Uso:
    python scrapers/catalunya.py
    python scrapers/catalunya.py --expediente "EXP-CAT-2026"
    python scrapers/catalunya.py --limite 5 --headless
    python scrapers/catalunya.py --stats
"""

import argparse
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
from urllib.parse import urljoin

from base import (
    PortalBase, ejecutar_portal, args_comunes,
    get_domain, clasificar_tipo, nombre_unico, capturar_pagina_pdf,
    WAIT_TIMEOUT,
)

DOMINIO = 'contractaciopublica.cat'

WAIT_CSS = '.navegacio-fases, button.btn-link, [class*="document"]'

DOMINIOS_FICHEROS_CAT = [
    'contractaciopublica.cat',
    'gestio.contractaciopublica.cat',
    'aplicaciones.justicia.cat',
]

# XPath para las fases accesibles.
# No filtramos por clase "visitada" porque la fase ACTIVA (en la que estamos)
# puede tener clase "actual" u otra — filtramos simplemente por si tiene <a href> real.
XPATH_FASES = (
    "//ul[contains(@class,'navegacio-fases')]"
    "//div[contains(@class,'navegacio-fases-fase')]"
    "//a[@href and string-length(@href) > 1 and not(starts-with(@href,'javascript'))]"
)

# Extensiones de fichero que descargamos
EXTS_DOC = ('.pdf', '.docx', '.doc', '.xls', '.xlsx', '.zip', '.xml', '.odt', '.json')

# Tiempo máximo de espera por descarga (segundos)
TIMEOUT_DESCARGA = 20


class PortalCatalunya(PortalBase):

    DOMINIO = DOMINIO

    def navegar(self, url):
        self.driver.get(url)
        try:
            WebDriverWait(self.driver, WAIT_TIMEOUT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, WAIT_CSS))
            )
        except Exception:
            pass
        time.sleep(4)
        self._cookies(get_domain(url))

    def _navegar_fase(self, url):
        self.driver.get(url)
        try:
            WebDriverWait(self.driver, WAIT_TIMEOUT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, WAIT_CSS))
            )
        except Exception:
            pass
        time.sleep(3)

    # ------------------------------------------------------------------
    # Detección de fases accesibles
    # ------------------------------------------------------------------

    def _obtener_fases(self, base_url):
        """
        Devuelve [(texto, url)] de todas las fases con clase
        navegacio-fases-fase-visitada, excluyendo la URL actual.
        """
        fases, vistos = [], set()

        # Selenium XPath (rápido)
        try:
            for el in self.driver.find_elements(By.XPATH, XPATH_FASES):
                href = el.get_attribute('href') or ''
                if not href or href == '#':
                    continue
                href_abs = urljoin(base_url, href)
                if href_abs in vistos:
                    continue
                if not any(d in href_abs for d in DOMINIOS_FICHEROS_CAT):
                    continue
                vistos.add(href_abs)
                fases.append((el.text.strip() or href_abs, href_abs))
        except Exception:
            pass

        # Fallback BeautifulSoup
        if not fases:
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            for div in soup.find_all('div', class_=lambda c: c and 'navegacio-fases-fase' in c):
                tag = div.find('a', href=True)
                if not tag:
                    continue
                href = tag['href']
                if not href or href.startswith('javascript') or href == '#':
                    continue
                href_abs = urljoin(base_url, href)
                if href_abs in vistos:
                    continue
                vistos.add(href_abs)
                fases.append((tag.get_text(strip=True) or href_abs, href_abs))

        # Quitar la URL principal (ya estamos en ella)
        fases = [(t, u) for t, u in fases if u.rstrip('/') != base_url.rstrip('/')]
        return fases

    # ------------------------------------------------------------------
    # Descarga por click de botón
    # ------------------------------------------------------------------

    def _snapshot(self, exp_dir: Path) -> dict:
        """Devuelve {path: mtime} de todos los ficheros en exp_dir."""
        if not exp_dir.exists():
            return {}
        return {f: f.stat().st_mtime for f in exp_dir.iterdir()}

    def _fichero_nuevo(self, antes: dict, exp_dir: Path):
        """
        Compara el estado actual de exp_dir con 'antes'.
        Devuelve el Path del fichero nuevo/modificado (no .crdownload), o None.
        """
        if not exp_dir.exists():
            return None
        for f in exp_dir.iterdir():
            if f.name.endswith('.crdownload'):
                continue
            mtime = f.stat().st_mtime
            if f not in antes or antes[f] != mtime:
                return f
        return None

    def _click_y_esperar(self, btn, exp_dir: Path) -> Path | None:
        """
        Hace click en el botón y espera hasta TIMEOUT_DESCARGA segundos
        a que aparezca un fichero nuevo/modificado en exp_dir.
        Devuelve el Path del fichero descargado, o None si timeout.
        """
        antes = self._snapshot(exp_dir)
        try:
            self.driver.execute_script('arguments[0].scrollIntoView({block:"center"});', btn)
            time.sleep(0.2)
            self.driver.execute_script('arguments[0].click();', btn)
        except Exception as e:
            print(f'        [click ERR] {e}')
            return None

        for _ in range(TIMEOUT_DESCARGA):
            time.sleep(1)
            nuevo = self._fichero_nuevo(antes, exp_dir)
            if nuevo:
                return nuevo
        return None


    def _descargar_botones_pagina(self, exp_dir: Path, ya_descargados: set,
                                   urls_desc: set) -> list:
        """
        Encuentra y descarga todos los botones de la página actual:
          1. Botones de evidencia (aria-label con "PDF"/"JSON"/"acreditada")
          2. Botones de documento (texto del botón = nombre de fichero)

        ya_descargados: set de nombres de fichero ya descargados esta sesión
        urls_desc:      set de 'boton:{nombre}' ya guardados en BD

        Retorna lista de (ruta, tipo, url_doc, error).
        """
        resultados = []
        exp_dir.mkdir(parents=True, exist_ok=True)

        # Configurar carpeta de descarga via CDP
        try:
            self.driver.execute_cdp_cmd('Page.setDownloadBehavior', {
                'behavior': 'allow',
                'downloadPath': str(exp_dir),
            })
        except Exception as e:
            print(f'      [CDP warn] {e}')

        botones = self.driver.find_elements(By.CSS_SELECTOR, 'button.btn-link')
        cola = []  # [(tipo, nombre_clave, btn)]

        for btn in botones:
            aria = (btn.get_attribute('aria-label') or '').lower()
            txt  = btn.text.strip()
            txt_low = txt.lower()

            # — Botones de evidencia —
            if ('pdf' in aria and ('descarr' in aria or 'descar' in aria)):
                cola.append(('evidencia_publicacion', 'evidencia_pdf', btn))
            elif ('json' in aria and ('descarr' in aria or 'descar' in aria)):
                cola.append(('evidencia_json', 'evidencia_json', btn))
            elif 'acreditada' in aria or 'sello' in txt_low or 'segell' in txt_low:
                cola.append(('sello_tiempo', 'sello_tiempo', btn))
            # — Botones de documento (nombre del fichero en el texto) —
            elif txt and any(txt_low.endswith(ext) for ext in EXTS_DOC):
                tipo = clasificar_tipo(txt, txt)
                cola.append((tipo, txt, btn))

        if cola:
            print(f'      [{len(cola)} botones detectados]')

        for tipo, nombre, btn in cola:
            boton_url = f'boton:{nombre}'

            # Saltar si ya descargado esta sesión o ya está en BD
            if nombre in ya_descargados or boton_url in urls_desc:
                continue

            # El botón de evidencia PDF llama a window.print() → bloquearía el navegador.
            # En su lugar capturamos la página actual con CDP (mismo resultado).
            if tipo == 'evidencia_publicacion':
                dest = nombre_unico(exp_dir, 'evidencia_publicacion', 'evidencia_publicacion.pdf')
                ok, err = capturar_pagina_pdf(self.driver, str(dest))
                if ok:
                    print(f'      OK  {tipo:30} {dest.name}')
                    resultados.append((str(dest), tipo, boton_url, None))
                    ya_descargados.add(nombre)
                else:
                    print(f'      WARN evidencia PDF: {err}')
                continue

            # Resto de botones: descarga directa de fichero
            nuevo = self._click_y_esperar(btn, exp_dir)

            if nuevo and nuevo.exists():
                print(f'      OK  {tipo:30} {nuevo.name}')
                resultados.append((str(nuevo), tipo, boton_url, None))
                ya_descargados.add(nombre)
            else:
                print(f'      WARN sin descarga: {nombre}')

        # Restaurar comportamiento de descarga
        try:
            self.driver.execute_cdp_cmd('Page.setDownloadBehavior', {'behavior': 'default'})
        except Exception:
            pass

        return resultados

    # ------------------------------------------------------------------
    # Método principal
    # ------------------------------------------------------------------

    def procesar(self, entry_link, expediente, exp_dir, urls_desc):
        """
        1. Abre la URL principal y detecta las fases accesibles
        2. En cada fase (y en la principal): click en todos los botones de descarga
        3. Deduplicación por nombre de fichero entre fases
        4. Fallback: si nada descargado, captura página como PDF
        """
        self.navegar(entry_link)
        fases = self._obtener_fases(entry_link)

        if fases:
            print(f'      [fases] principal + {len(fases)} fases: {[t for t,_ in fases]}')
        else:
            print('      [fases] solo página principal')

        ya_descargados = set()   # nombres de fichero descargados esta sesión
        todos = []

        # Página principal
        r = self._descargar_botones_pagina(exp_dir, ya_descargados, urls_desc)
        todos.extend(r)

        # Fases adicionales
        for texto, url_fase in fases:
            print(f'      [fase] {texto}')
            try:
                self._navegar_fase(url_fase)
                r = self._descargar_botones_pagina(exp_dir, ya_descargados, urls_desc)
                todos.extend(r)
            except Exception as e:
                print(f'      [fase ERR] {texto}: {e}')

        # Fallback: capturar página como PDF si no hubo ninguna descarga
        if not any(r[0] for r in todos):
            print('      [sin docs] capturando página como PDF...')
            if self.driver.current_url.rstrip('/') != entry_link.rstrip('/'):
                self.navegar(entry_link)
            dest = nombre_unico(exp_dir, 'ficha_expediente', 'ficha_catalunya.pdf')
            ok, err = capturar_pagina_pdf(self.driver, str(dest))
            if ok:
                print(f'      OK  ficha_expediente             {dest.name}')
                todos.append((str(dest), 'ficha_expediente', entry_link, None))
            else:
                print(f'      ERR captura PDF: {err}')

        return todos


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Scraper contractaciopublica.cat (Generalitat de Catalunya)'
    )
    args_comunes(parser)
    args = parser.parse_args()
    ejecutar_portal(PortalCatalunya, DOMINIO, args)
