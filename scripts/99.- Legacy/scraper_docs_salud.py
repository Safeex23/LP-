"""
scraper_docs_salud.py — Descarga incremental de documentos via Selenium
========================================================================
Diseño incremental/histórico:
  - En cada ejecución visita todos los expedientes configurados.
  - Por cada documento encontrado en el portal comprueba si ya está en BD
    (por doc_url). Si ya está, lo salta. Si es nuevo, lo descarga e inserta.
  - Nunca borra registros anteriores: los documentos viejos se conservan.
  - Si un expediente pasa de 'Evaluacion' a 'Resuelta' en 5 meses, al
    re-lanzar encontrará el PDF de adjudicación nuevo y lo añadirá.

Portales soportados:
  1. contrataciondelestado.es   (cubre 15 CCAAs)
  2. contratacion.euskadi.eus
  3. contratos-publicos.comunidad.madrid
  4. contractaciopublica.cat
  5. juntadeandalucia.es
  6. hacienda.navarra.es

Formatos descargados: .pdf, .doc, .docx, .xml, .odt, .xls, .xlsx, .zip

Uso:
  python salud/scripts/scraper_docs_salud.py             # todos
  python salud/scripts/scraper_docs_salud.py --portal euskadi
  python salud/scripts/scraper_docs_salud.py --expediente "2025/02416"
  python salud/scripts/scraper_docs_salud.py --headless
  python salud/scripts/scraper_docs_salud.py --stats

Requisitos:
  pip install selenium webdriver-manager beautifulsoup4 requests
"""

import argparse
import base64
import os
import re
import sqlite3
import time
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlparse, urljoin, unquote

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

RUTA_DB   = r'C:\proyectos\licitaciones\salud\datos\licitaciones_salud.db'
RUTA_PDFS = r'C:\proyectos\licitaciones\salud\datos\pdfs'

WAIT_TIMEOUT       = 15
DELAY_ENTRE_EXPTES = 2

EXTENSIONES_DOCUMENTO = {'.pdf', '.doc', '.docx', '.xml', '.odt', '.xls', '.xlsx', '.zip'}
# HTML solo se captura via CDP (capturar_pagina_pdf), no como descarga directa


# ---------------------------------------------------------------------------
# Helpers generales
# ---------------------------------------------------------------------------
def sanitize(name, max_len=120):
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', str(name))
    name = re.sub(r'\s+', '_', name).strip('._')
    return name[:max_len] or 'doc'


def get_domain(url):
    try:
        return urlparse(url or '').netloc
    except Exception:
        return ''


def ext_valida(href):
    """True si la URL apunta a un formato de documento válido."""
    path = urlparse(href).path.lower().split('?')[0]
    if any(path.endswith(ext) for ext in EXTENSIONES_DOCUMENTO):
        return True
    return False


def parece_descarga(href):
    """True si la URL parece un endpoint de descarga aunque no tenga extensión."""
    low = href.lower()
    patrones = [
        'getdocument', 'getfile', 'download', 'descarga', 'filedownload',
        'fileystem', 'servlet', 'adjunto', 'attachment', 'documento',
        'getdocumentbyid', 'filesystem',
    ]
    return any(p in low for p in patrones)


# Patrones que indican que un enlace es navegación del portal, no un documento
_EXCLUIR_NAVEGACION = [
    '/temas/', '/inicio', '/home', '/portal/', '/buscador', '/ayuda',
    '/accesibilidad', '/aviso-legal', '/contacto', '/mapa-web',
    '/ca/', '/eu/', '/gl/', '/va/',      # idiomas
    'estructura-anuncios-xml',           # doc tecnico del portal, no del exp
    'manual-', 'guia-', 'tutorial',
    'javascript:', 'mailto:',
]


def es_navegacion_portal(href):
    """True si la URL es claramente un enlace de navegación, no un documento."""
    low = href.lower()
    return any(p in low for p in _EXCLUIR_NAVEGACION)


def capturar_pagina_pdf(driver, dest_path):
    """
    Imprime la página actual del driver a PDF via CDP (sin diálogo de impresión).
    Útil para portales que publican el anuncio como HTML (ej: Andalucía).
    """
    try:
        # Bloquear el dialogo nativo de impresion por si acaso
        driver.execute_script('window.print = function() {};')
        result = driver.execute_cdp_cmd('Page.printToPDF', {
            'landscape': False,
            'printBackground': True,
            'paperWidth': 8.27,   # A4
            'paperHeight': 11.69,
            'marginTop': 0.5,
            'marginBottom': 0.5,
            'marginLeft': 0.5,
            'marginRight': 0.5,
            'scale': 1.0,
        })
        pdf_bytes = base64.b64decode(result['data'])
        if len(pdf_bytes) < 500:
            return False, 'PDF generado vacio'
        with open(dest_path, 'wb') as f:
            f.write(pdf_bytes)
        return True, None
    except Exception as e:
        return False, str(e)[:100]


def descargar_binario(url, dest_path, timeout=30, retries=2):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'}
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, timeout=timeout, stream=True, headers=headers)
            if r.status_code == 200:
                # Comprobar que es contenido real (no HTML de error)
                content_type = r.headers.get('content-type', '').lower()
                if 'text/html' in content_type and not url.lower().endswith('.html'):
                    return False, 'Respuesta HTML (no es un fichero)'
                with open(dest_path, 'wb') as f:
                    for chunk in r.iter_content(8192):
                        f.write(chunk)
                # Comprobar tamanno minimo (evitar ficheros vacios / error pages)
                size = os.path.getsize(dest_path)
                if size < 500:
                    os.remove(dest_path)
                    return False, f'Fichero demasiado pequeño ({size} bytes)'
                return True, None
            elif r.status_code == 404:
                return False, 'HTTP 404'
            else:
                if attempt < retries:
                    time.sleep(2 ** attempt)
                else:
                    return False, f'HTTP {r.status_code}'
        except requests.exceptions.Timeout:
            if attempt < retries:
                time.sleep(2 ** attempt)
            else:
                return False, 'Timeout'
        except Exception as e:
            if attempt < retries:
                time.sleep(2 ** attempt)
            else:
                return False, str(e)[:100]
    return False, 'Max reintentos'


def nombre_destino_unico(exp_dir, tipo, filename_base):
    """Genera ruta única: si ya existe el fichero añade sufijo _01, _02..."""
    base = f'selenium__{sanitize(tipo)}__{sanitize(filename_base)}'
    dest = exp_dir / base
    if not dest.exists():
        return dest
    stem = dest.stem
    suffix = dest.suffix or '.pdf'
    for i in range(1, 100):
        candidate = exp_dir / f'{stem}__{i:02d}{suffix}'
        if not candidate.exists():
            return candidate
    return exp_dir / f'{stem}__x{suffix}'


def inferir_nombre_fichero(url, texto_enlace, idx):
    """Obtiene nombre de fichero desde URL o texto del enlace."""
    # 1. Desde la URL
    path = unquote(urlparse(url).path)
    basename = os.path.basename(path)
    if basename and any(basename.lower().endswith(ext) for ext in EXTENSIONES_DOCUMENTO):
        return basename
    # 2. Desde query string (ej: filename=xxx.pdf)
    qs = urlparse(url).query
    for part in qs.split('&'):
        if '=' in part:
            k, v = part.split('=', 1)
            if k.lower() in ('filename', 'name', 'file', 'doc'):
                v_dec = unquote(v)
                if v_dec:
                    return v_dec
    # 3. Desde texto del enlace
    if texto_enlace:
        clean = re.sub(r'[<>:"/\\|?*]', '', texto_enlace)[:60].strip()
        if clean:
            return clean + '.pdf'
    # 4. Fallback
    return f'doc_{idx:02d}.pdf'


# ---------------------------------------------------------------------------
# Clasificacion de tipo de documento
# ---------------------------------------------------------------------------
MAP_TIPO_KEYWORDS = {
    'pliego_administrativo': [
        'pcap', 'pliego de clausulas', 'pliego administrativo',
        'condiciones administrativas', 'clausulas administrativas',
    ],
    'pliego_tecnico': [
        'ppt', 'prescripciones tecnicas', 'pliego tecnico',
        'especificaciones tecnicas', 'condiciones tecnicas',
    ],
    'anuncio_DOC_CAN_ADJ': [
        'adjudicacion', 'resolucion adjudicacion', 'acuerdo adjudicacion',
        'resolucion de adjudicacion', 'adj ',
    ],
    'anuncio_DOC_CN': [
        'convocatoria', 'anuncio de licitacion', 'anuncio convocatoria',
        'boe', 'doue', 'bop', 'bocm', 'boja', 'dogc',
    ],
    'anuncio_DOC_FORM': [
        'formalizacion', 'contrato formalizado', 'documento de formalizacion',
    ],
    'anuncio_DOC_MOD': [
        'modificacion', 'modificado', 'modificacion del contrato',
    ],
    'anuncio_DOC_CD': [
        'desistimiento', 'renuncia',
    ],
    'anuncio_DOC_PIN': [
        'informacion previa', 'pin',
    ],
}


def clasificar_tipo(texto_contexto, url=''):
    """Clasifica tipo de documento desde texto del contexto + URL."""
    combined = (texto_contexto + ' ' + url).lower()
    combined = combined.replace('á','a').replace('é','e').replace('í','i').replace('ó','o').replace('ú','u')
    for tipo, keywords in MAP_TIPO_KEYWORDS.items():
        if any(kw in combined for kw in keywords):
            return tipo
    return 'documento_descargado'


# ---------------------------------------------------------------------------
# Aceptar cookies
# ---------------------------------------------------------------------------
def aceptar_cookies(driver, timeout=5):
    selectores_xpath = [
        "//*[@id='onetrust-accept-btn-handler']",
        "//button[contains(translate(text(),'ACEPTAR','aceptar'),'aceptar')]",
        "//button[contains(translate(text(),'ACEPTO','acepto'),'acepto')]",
        "//button[contains(translate(text(),'AGREE','agree'),'agree')]",
        "//span[contains(translate(text(),'ACEPTAR','aceptar'),'aceptar')]",
        "//*[contains(@class,'cookie') and contains(translate(.,'ACEPTAR','aceptar'),'aceptar')]",
        "//*[contains(@id,'accept')]",
        "//*[contains(@class,'accept')]",
    ]
    for xpath in selectores_xpath:
        try:
            btn = WebDriverWait(driver, timeout).until(
                EC.element_to_be_clickable((By.XPATH, xpath))
            )
            btn.click()
            time.sleep(1)
            return True
        except Exception:
            continue
    return False


# ---------------------------------------------------------------------------
# Inicializar Chrome
# ---------------------------------------------------------------------------
def crear_driver(headless=False):
    options = Options()
    if headless:
        options.add_argument('--headless=new')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--window-size=1400,900')
    options.add_argument('--disable-blink-features=AutomationDetection')
    options.add_argument(
        '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    )
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(30)
    return driver


# ---------------------------------------------------------------------------
# Clase base — handler de portal
# ---------------------------------------------------------------------------
class PortalBase:
    nombre = 'generico'

    def __init__(self, driver):
        self.driver = driver
        self._cookies_aceptadas = set()

    def _cookies(self, dominio):
        if dominio not in self._cookies_aceptadas:
            if aceptar_cookies(self.driver, timeout=4):
                print(f'    [cookies OK] {dominio}')
            self._cookies_aceptadas.add(dominio)

    def navegar(self, url, wait_css=None, extra_sleep=2):
        self.driver.get(url)
        if wait_css:
            try:
                WebDriverWait(self.driver, WAIT_TIMEOUT).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, wait_css))
                )
            except Exception:
                pass
        time.sleep(extra_sleep)
        self._cookies(get_domain(url))

    def obtener_enlaces(self, base_url):
        """
        Devuelve lista de dicts:
          {url, texto, contexto, tipo}
        donde 'contexto' es texto del elemento padre (para clasificar mejor).
        """
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        base_domain = get_domain(base_url)
        encontrados = []
        vistos = set()

        for tag in soup.find_all('a', href=True):
            href = tag['href'].strip()
            if not href or href.startswith('#') or href.startswith('javascript'):
                continue
            href_abs = urljoin(base_url, href)
            parsed = urlparse(href_abs)
            # Permitir mismo dominio o URLs absolutas de descarga conocidas
            if parsed.netloc and parsed.netloc != base_domain:
                # Excepciones: dominios de descarga conocidos del portal estatal
                if not any(d in parsed.netloc for d in ['contrataciondelestado.es', 'contratacion']):
                    continue
            if not (ext_valida(href_abs) or parece_descarga(href_abs)):
                continue
            if es_navegacion_portal(href_abs):
                continue
            if href_abs in vistos:
                continue
            vistos.add(href_abs)

            texto = tag.get_text(strip=True)
            # Contexto: texto del padre o fila de tabla que contiene el enlace
            contexto = self._contexto_enlace(tag)
            tipo = clasificar_tipo(contexto + ' ' + texto, href_abs)

            encontrados.append({
                'url': href_abs,
                'texto': texto,
                'contexto': contexto,
                'tipo': tipo,
            })

        return encontrados

    def _contexto_enlace(self, tag):
        """Extrae texto del contexto más cercano (fila de tabla, div, li...)."""
        for parent_tag in ['tr', 'li', 'div', 'td', 'p', 'span']:
            parent = tag.find_parent(parent_tag)
            if parent:
                txt = parent.get_text(separator=' ', strip=True)
                if len(txt) > 5:
                    return txt[:300]
        return ''

    def procesar(self, entry_link, expediente, exp_dir, urls_ya_descargadas):
        self.navegar(entry_link)
        enlaces = self.obtener_enlaces(entry_link)
        return self._descargar_nuevos(enlaces, exp_dir, urls_ya_descargadas)

    def _descargar_nuevos(self, enlaces, exp_dir, urls_ya_descargadas):
        """
        Descarga solo los enlaces cuya URL no está ya en urls_ya_descargadas.
        Devuelve lista de (ruta_local, tipo, url, error).
        """
        resultados = []
        for idx, enlace in enumerate(enlaces):
            url = enlace['url']
            if url in urls_ya_descargadas:
                print(f'      [ya descargado] {url[:70]}')
                continue

            filename = inferir_nombre_fichero(url, enlace['texto'], idx)
            dest = nombre_destino_unico(exp_dir, enlace['tipo'], filename)

            ok, err = descargar_binario(url, str(dest))
            if ok:
                print(f'      OK  {enlace["tipo"]:<28} {dest.name}')
                resultados.append((str(dest), enlace['tipo'], url, None))
            else:
                print(f'      ERR {enlace["tipo"]:<28} {err}')
                resultados.append((None, enlace['tipo'], url, err))

        return resultados


# ---------------------------------------------------------------------------
# Portal: contrataciondelestado.es
# ---------------------------------------------------------------------------
class PortalContratacionEstado(PortalBase):
    nombre = 'contrataciondelestado.es'

    def navegar(self, url, **kw):
        self.driver.get(url)
        # Esperar tabla de documentos (JS pesado)
        try:
            WebDriverWait(self.driver, WAIT_TIMEOUT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR,
                    'table, .documentos, [class*="docum"], .fichero, a[href*="GetDocument"]'))
            )
        except Exception:
            pass
        time.sleep(3)
        self._cookies(get_domain(url))

    def obtener_enlaces(self, base_url):
        """
        El portal estatal muestra los documentos en una tabla.
        Cada fila tiene: [icono tipo] [nombre doc] [botón descarga]
        Capturamos el texto de toda la fila como contexto para clasificar.
        """
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        encontrados = []
        vistos = set()

        for tag in soup.find_all('a', href=True):
            href = tag['href'].strip()
            if not href:
                continue
            href_abs = urljoin(base_url, href)

            # Solo enlaces de descarga del portal estatal
            es_descarga = (
                'GetDocumentByIdServlet' in href or
                'FileSystem/servlet' in href or
                'fichero' in href.lower() or
                ext_valida(href_abs)
            )
            if not es_descarga:
                continue
            if href_abs in vistos:
                continue
            vistos.add(href_abs)

            texto = tag.get_text(strip=True)
            # Buscar fila de tabla padre para extraer label del tipo de documento
            contexto = ''
            tr = tag.find_parent('tr')
            if tr:
                # El label del tipo suele estar en la primera columna
                tds = tr.find_all('td')
                contexto = ' '.join(td.get_text(strip=True) for td in tds)
            else:
                contexto = self._contexto_enlace(tag)

            tipo = clasificar_tipo(contexto + ' ' + texto, href_abs)
            encontrados.append({'url': href_abs, 'texto': texto, 'contexto': contexto, 'tipo': tipo})

        return encontrados


# ---------------------------------------------------------------------------
# Portal: contratacion.euskadi.eus
# ---------------------------------------------------------------------------
class PortalEuskadi(PortalBase):
    nombre = 'contratacion.euskadi.eus'

    def navegar(self, url, **kw):
        self.driver.get(url)
        time.sleep(2)
        self._cookies(get_domain(url))
        # Hacer click en la pestana "Documentos" si existe
        self._abrir_pestana_documentos()
        time.sleep(2)

    def _abrir_pestana_documentos(self):
        """Busca y hace click en la pestaña de documentos del portal vasco."""
        selectores = [
            "//a[contains(translate(text(),'DOCUMENTOS','documentos'),'documentos')]",
            "//li[contains(@class,'documents')]//a",
            "//a[contains(@href,'doc')]",
            "//*[@id='tab-documents']",
            "//*[contains(@class,'tab') and contains(translate(.,'DOCUMENTOS','documentos'),'documentos')]",
        ]
        for xpath in selectores:
            try:
                btn = WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, xpath))
                )
                btn.click()
                time.sleep(2)
                print('      [tab documentos OK]')
                return True
            except Exception:
                continue
        return False

    def obtener_enlaces(self, base_url):
        """
        El portal vasco tiene una tabla de documentos con columnas:
        Tipo | Nombre | Fecha | Acciones (descarga)
        """
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        encontrados = []
        vistos = set()

        for tag in soup.find_all('a', href=True):
            href = tag['href'].strip()
            if not href:
                continue
            href_abs = urljoin(base_url, href)
            if not (ext_valida(href_abs) or parece_descarga(href_abs)):
                continue
            # Excluir links de navegación del portal
            if any(skip in href_abs.lower() for skip in ['/es/', '/eu/', 'index.html', '/ca/']):
                # Solo saltar si no es un fichero concreto
                path_low = urlparse(href_abs).path.lower()
                if not any(path_low.endswith(ext) for ext in EXTENSIONES_DOCUMENTO):
                    continue
            if href_abs in vistos:
                continue
            vistos.add(href_abs)

            texto = tag.get_text(strip=True)
            contexto = self._contexto_enlace(tag)
            tipo = clasificar_tipo(contexto + ' ' + texto, href_abs)
            encontrados.append({'url': href_abs, 'texto': texto, 'contexto': contexto, 'tipo': tipo})

        return encontrados


# ---------------------------------------------------------------------------
# Portal: contratos-publicos.comunidad.madrid
# ---------------------------------------------------------------------------
class PortalMadrid(PortalBase):
    nombre = 'contratos-publicos.comunidad.madrid'

    def navegar(self, url, **kw):
        self.driver.get(url)
        try:
            WebDriverWait(self.driver, WAIT_TIMEOUT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR,
                    '.field--name-field-doc, [class*="documentacion"], a[href*="download"]'))
            )
        except Exception:
            pass
        time.sleep(3)
        self._cookies(get_domain(url))

    def obtener_enlaces(self, base_url):
        """
        Madrid muestra los documentos en divs con label visible antes del enlace.
        Estructura típica: <div class="field__label">Pliego...</div> <a href=".../download">
        """
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        encontrados = []
        vistos = set()

        for tag in soup.find_all('a', href=True):
            href = tag['href'].strip()
            if not href:
                continue
            href_abs = urljoin(base_url, href)
            if not (ext_valida(href_abs) or parece_descarga(href_abs)):
                continue
            if href_abs in vistos:
                continue
            vistos.add(href_abs)

            texto = tag.get_text(strip=True)
            # Buscar label del campo anterior (patrón Drupal de Madrid)
            contexto = ''
            campo = tag.find_parent(class_=re.compile(r'field|documento|doc'))
            if campo:
                label = campo.find(class_=re.compile(r'label|title|tipo'))
                contexto = (label.get_text(strip=True) if label else '') + ' ' + campo.get_text(separator=' ', strip=True)[:200]
            else:
                contexto = self._contexto_enlace(tag)

            tipo = clasificar_tipo(contexto + ' ' + texto, href_abs)
            encontrados.append({'url': href_abs, 'texto': texto, 'contexto': contexto, 'tipo': tipo})

        return encontrados


# ---------------------------------------------------------------------------
# Portal: contractaciopublica.cat
# ---------------------------------------------------------------------------
class PortalCatalunya(PortalBase):
    nombre = 'contractaciopublica.cat'

    def navegar(self, url, **kw):
        self.driver.get(url)
        try:
            WebDriverWait(self.driver, WAIT_TIMEOUT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR,
                    '.documents, .adjunts, a[href$=".pdf"], a[href$=".doc"], a[href$=".docx"], a[href$=".xml"], a[href$=".odt"], a[href$=".xls"], a[href$=".xlsx"], a[href$=".zip"]'))
            )
        except Exception:
            pass
        time.sleep(3)
        self._cookies(get_domain(url))


# ---------------------------------------------------------------------------
# Portal: juntadeandalucia.es
# ---------------------------------------------------------------------------
class PortalAndalucia(PortalBase):
    """
    juntadeandalucia.es — JSF.
    La ficha de licitación raramente expone PDFs directos.
    Estrategia: intentar encontrar enlaces descargables y, si no hay ninguno,
    capturar la página completa como PDF (contiene toda la info del anuncio).
    """
    nombre = 'juntadeandalucia.es'

    def navegar(self, url, **kw):
        self.driver.get(url)
        try:
            WebDriverWait(self.driver, WAIT_TIMEOUT + 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR,
                    'a[href*=".pdf"], a[href*="documento"], .documentos, form, table'))
            )
        except Exception:
            pass
        time.sleep(5)
        self._cookies(get_domain(url))

    def procesar(self, entry_link, expediente, exp_dir, urls_ya_descargadas):
        self.navegar(entry_link)
        enlaces = self.obtener_enlaces(entry_link)
        resultados = self._descargar_nuevos(enlaces, exp_dir, urls_ya_descargadas)

        # Si no se encontraron ficheros descargables, capturar la página como PDF
        # (el anuncio JSF contiene toda la info relevante de la licitacion)
        url_pagina = self.driver.current_url
        if not any(r[0] for r in resultados) and url_pagina not in urls_ya_descargadas:
            print('      [sin ficheros] capturando pagina HTML como PDF...')
            dest = nombre_destino_unico(exp_dir, 'anuncio_pagina_web', 'ficha_licitacion.pdf')
            ok, err = capturar_pagina_pdf(self.driver, str(dest))
            if ok:
                print(f'      OK  anuncio_pagina_web          {dest.name}')
                resultados.append((str(dest), 'anuncio_pagina_web', url_pagina, None))
            else:
                print(f'      ERR captura PDF: {err}')
                resultados.append((None, 'anuncio_pagina_web', url_pagina, err))

        return resultados


# ---------------------------------------------------------------------------
# Portal: hacienda.navarra.es
# ---------------------------------------------------------------------------
class PortalNavarra(PortalBase):
    nombre = 'hacienda.navarra.es'

    def navegar(self, url, **kw):
        self.driver.get(url)
        try:
            WebDriverWait(self.driver, WAIT_TIMEOUT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'a[href$=".pdf"], table'))
            )
        except Exception:
            pass
        time.sleep(3)
        self._cookies(get_domain(url))


# ---------------------------------------------------------------------------
# Mapa dominio -> handler
# ---------------------------------------------------------------------------
PORTAL_MAP = {
    'contrataciondelestado.es':                  PortalContratacionEstado,
    'www.contrataciondelestado.es':               PortalContratacionEstado,
    'contratacion.euskadi.eus':                   PortalEuskadi,
    'www.contratacion.euskadi.eus':               PortalEuskadi,
    'contratos-publicos.comunidad.madrid':        PortalMadrid,
    'www.contratos-publicos.comunidad.madrid':    PortalMadrid,
    'contractaciopublica.cat':                    PortalCatalunya,
    'www.contractaciopublica.cat':                PortalCatalunya,
    'juntadeandalucia.es':                        PortalAndalucia,
    'www.juntadeandalucia.es':                    PortalAndalucia,
    'hacienda.navarra.es':                        PortalNavarra,
    'www.hacienda.navarra.es':                    PortalNavarra,
}


def get_handler(entry_link, driver):
    cls = PORTAL_MAP.get(get_domain(entry_link), PortalBase)
    return cls(driver)


# ---------------------------------------------------------------------------
# Bucle principal — lógica incremental
# ---------------------------------------------------------------------------
def procesar_todo(conn, portal_filtro=None, expediente_filtro=None,
                  headless=False, limite=None, delay=DELAY_ENTRE_EXPTES,
                  solo_pendientes=False):
    """
    Itera sobre todos los expedientes con entry_link.
    Por cada uno comprueba qué documentos ya están en BD y solo descarga los nuevos.

    solo_pendientes=True: procesa solo expedientes con algún doc sin descargar
                   False: procesa todos (modo incremental completo — detecta novedades)
    """
    if solo_pendientes:
        # Modo rápido: solo expedientes con al menos 1 doc sin URL
        base_query = '''
            SELECT DISTINCT l.expediente, l.entry_link, l.org_nombre, l.ccaa, l.estado
            FROM licitaciones l
            JOIN documentos d ON d.expediente = l.expediente
            WHERE (d.doc_url IS NULL OR d.doc_url = '') AND d.descargado = 0
              AND (l.entry_link IS NOT NULL AND l.entry_link != '')
        '''
    else:
        # Modo incremental: todos los expedientes con entry_link
        base_query = '''
            SELECT DISTINCT l.expediente, l.entry_link, l.org_nombre, l.ccaa, l.estado
            FROM licitaciones l
            WHERE l.entry_link IS NOT NULL AND l.entry_link != ''
        '''

    params = []
    if expediente_filtro:
        base_query += ' AND l.expediente = ?'
        params.append(expediente_filtro)
    if limite:
        base_query += f' LIMIT {limite}'

    exptes = conn.execute(base_query, params).fetchall()

    if portal_filtro:
        exptes = [e for e in exptes if portal_filtro.lower() in get_domain(e[1] or '').lower()]

    total = len(exptes)
    print(f'\nExpedientes a procesar: {total}')
    if total == 0:
        print('Nada que procesar.')
        return

    driver = crear_driver(headless=headless)
    print(f'Chrome iniciado (headless={headless})')

    ok_exptes = err_exptes = nuevos_docs = 0
    handler_cache = {}

    def _reiniciar_driver():
        """Cierra Chrome si sigue vivo y crea uno nuevo."""
        nonlocal driver, handler_cache
        try:
            driver.quit()
        except Exception:
            pass
        time.sleep(2)
        driver = crear_driver(headless=headless)
        handler_cache = {}  # los handlers tienen ref al driver viejo, limpiar
        print('  [Chrome reiniciado]')

    def _es_sesion_invalida(exc):
        msg = str(exc).lower()
        return 'invalid session id' in msg or 'no such session' in msg or 'session deleted' in msg

    try:
        for i, (expediente, entry_link, org_nombre, ccaa, estado) in enumerate(exptes, 1):
            dominio = get_domain(entry_link)
            print(f'\n[{i}/{total}] {expediente} | {dominio} | {ccaa or "?"} | {estado or "?"}')
            print(f'  Org: {(org_nombre or "")[:60]}')

            urls_ya_en_bd = set(
                r[0] for r in conn.execute(
                    "SELECT doc_url FROM documentos WHERE expediente=? AND doc_url IS NOT NULL AND doc_url != '' AND descargado=1",
                    (expediente,)
                ).fetchall()
            )
            print(f'  Docs ya en BD: {len(urls_ya_en_bd)}')

            exp_dir = Path(RUTA_PDFS) / sanitize(expediente)
            exp_dir.mkdir(parents=True, exist_ok=True)

            # Reusar handler del mismo dominio; si Chrome se reinició, recrear
            if dominio not in handler_cache:
                handler_cache[dominio] = get_handler(entry_link, driver)
            else:
                # Actualizar referencia al driver en el handler (por si hubo reinicio)
                handler_cache[dominio].driver = driver
            handler = handler_cache[dominio]

            intentos = 0
            while intentos < 2:
                try:
                    resultados = handler.procesar(entry_link, expediente, exp_dir, urls_ya_en_bd)
                    break
                except Exception as e:
                    if _es_sesion_invalida(e) and intentos == 0:
                        print(f'  [Chrome caido] Reiniciando...')
                        _reiniciar_driver()
                        handler_cache[dominio] = get_handler(entry_link, driver)
                        handler = handler_cache[dominio]
                        intentos += 1
                    else:
                        print(f'  [ERROR] {str(e)[:120]}')
                        err_exptes += 1
                        resultados = []
                        break
            else:
                resultados = []

            n_nuevos = 0
            for ruta_local, tipo_inf, doc_url_found, error in resultados:
                if ruta_local:
                    n_nuevos += 1
                    nuevos_docs += 1
                    fila = conn.execute('''
                        SELECT id FROM documentos
                        WHERE expediente=? AND tipo_documento=?
                          AND (doc_url IS NULL OR doc_url='') AND descargado=0
                        LIMIT 1
                    ''', (expediente, tipo_inf)).fetchone()
                    if fila:
                        conn.execute('''
                            UPDATE documentos
                            SET descargado=1, ruta_local=?, doc_url=?, error=''
                            WHERE id=?
                        ''', (ruta_local, doc_url_found, fila[0]))
                    else:
                        conn.execute('''
                            INSERT INTO documentos
                            (expediente, tipo_documento, doc_url, ruta_local, descargado, error)
                            VALUES (?,?,?,?,1,'')
                        ''', (expediente, tipo_inf, doc_url_found, ruta_local))
                elif error:
                    conn.execute('''
                        UPDATE documentos SET error=?
                        WHERE id = (
                            SELECT id FROM documentos
                            WHERE expediente=? AND tipo_documento=?
                              AND (doc_url IS NULL OR doc_url='') AND descargado=0
                            LIMIT 1
                        )
                    ''', (error, expediente, tipo_inf))

            conn.commit()
            if resultados:
                ok_exptes += 1
                print(f'  --> {n_nuevos} docs nuevos descargados')
            elif err_exptes == 0 or n_nuevos == 0:
                print(f'  [!] Sin documentos nuevos encontrados')

            time.sleep(delay)

    finally:
        try:
            driver.quit()
        except Exception:
            pass
        print('\nChrome cerrado.')

    print(f'\n{"="*55}')
    print(f'Selenium completado')
    print(f'  Expedientes OK    : {ok_exptes}')
    print(f'  Expedientes error : {err_exptes}')
    print(f'  Documentos nuevos : {nuevos_docs}')
    print(f'{"="*55}')


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------
def mostrar_stats(conn):
    total    = conn.execute('SELECT COUNT(*) FROM documentos').fetchone()[0]
    desc     = conn.execute('SELECT COUNT(*) FROM documentos WHERE descargado=1').fetchone()[0]
    sin_url  = conn.execute("SELECT COUNT(*) FROM documentos WHERE descargado=0 AND (doc_url IS NULL OR doc_url='')").fetchone()[0]
    selenium = conn.execute("SELECT COUNT(*) FROM documentos WHERE ruta_local LIKE '%selenium%'").fetchone()[0]
    direct   = conn.execute("SELECT COUNT(*) FROM documentos WHERE descargado=1 AND ruta_local NOT LIKE '%selenium%'").fetchone()[0]

    print('\n=== STATS documentos salud ===')
    print(f'  Total registros      : {total}')
    print(f'  Descargados total    : {desc}')
    print(f'    via descarga directa : {direct}')
    print(f'    via Selenium         : {selenium}')
    print(f'  Pendientes (sin URL) : {sin_url}')

    print('\n  Por tipo:')
    for tipo, tot, desc2 in conn.execute('''
        SELECT tipo_documento,
               COUNT(*) total,
               SUM(CASE WHEN descargado=1 THEN 1 ELSE 0 END) desc
        FROM documentos GROUP BY tipo_documento ORDER BY total DESC
    ''').fetchall():
        print(f'    {tipo:<32} total={tot:4d}  desc={desc2 or 0:4d}')

    print('\n  Por portal (pendientes):')
    rows = conn.execute('''
        SELECT l.entry_link, COUNT(d.id) pend
        FROM documentos d
        JOIN licitaciones l ON l.expediente=d.expediente
        WHERE d.descargado=0 AND (d.doc_url IS NULL OR d.doc_url='')
        GROUP BY l.entry_link
    ''').fetchall()
    by_dom = defaultdict(int)
    for r in rows:
        by_dom[get_domain(r[0] or '')] += r[1]
    for dom, cnt in sorted(by_dom.items(), key=lambda x: -x[1]):
        print(f'    {dom:<50} pendiente={cnt}')


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scraper Selenium incremental — licitaciones salud')
    parser.add_argument('--portal', default=None,
                        help='Filtrar por dominio (ej: euskadi, comunidad.madrid, contrataciondelestado)')
    parser.add_argument('--expediente', default=None,
                        help='Procesar solo este expediente')
    parser.add_argument('--limite', type=int, default=None,
                        help='Maximo numero de expedientes a procesar')
    parser.add_argument('--solo-pendientes', action='store_true',
                        help='Procesar solo expedientes con docs sin URL (mas rapido)')
    parser.add_argument('--headless', action='store_true',
                        help='Chrome sin ventana')
    parser.add_argument('--delay', type=float, default=DELAY_ENTRE_EXPTES,
                        help=f'Segundos entre expedientes (defecto: {DELAY_ENTRE_EXPTES})')
    parser.add_argument('--stats', action='store_true',
                        help='Solo mostrar estadisticas')
    args = parser.parse_args()

    conn = sqlite3.connect(RUTA_DB, timeout=30)
    conn.execute('PRAGMA journal_mode=WAL')

    if args.stats:
        mostrar_stats(conn)
        conn.close()
        exit(0)

    procesar_todo(
        conn,
        portal_filtro=args.portal,
        expediente_filtro=args.expediente,
        headless=args.headless,
        limite=args.limite,
        delay=args.delay,
        solo_pendientes=args.solo_pendientes,
    )
    conn.close()
    print(f'\nPDFs/docs en: {RUTA_PDFS}')
