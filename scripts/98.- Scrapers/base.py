"""
base.py — Utilidades comunes para todos los scrapers de portales de licitación
==============================================================================
Importar en cada scraper de portal:
    from scrapers.base import (
        PortalBase, crear_driver, get_db, sanitize,
        descargar_binario, capturar_pagina_pdf,
        RUTA_DB, RUTA_PDFS
    )
"""

import base64
import os
import re
import sqlite3
import time
from pathlib import Path
from urllib.parse import urlparse, urljoin, unquote

import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

# ---------------------------------------------------------------------------
# Rutas globales
# ---------------------------------------------------------------------------
RUTA_DB   = r'C:\proyectos\licitaciones\salud\datos\licitaciones_salud.db'
RUTA_PDFS = r'C:\proyectos\licitaciones\salud\datos\pdfs'

WAIT_TIMEOUT = 15

EXTENSIONES_DOCUMENTO = {
    '.pdf', '.doc', '.docx', '.xml', '.odt', '.xls', '.xlsx', '.zip'
}

# Palabras en URL que indican navegación del portal, NO un documento
_EXCLUIR_NAVEGACION = [
    '/temas/', '/inicio', '/home', '/portal/', '/buscador', '/ayuda',
    '/accesibilidad', '/aviso-legal', '/contacto', '/mapa-web',
    'estructura-anuncios-xml', 'manual-', 'guia-', 'tutorial',
]


# ---------------------------------------------------------------------------
# Helpers
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
    path = urlparse(href).path.lower().split('?')[0]
    return any(path.endswith(ext) for ext in EXTENSIONES_DOCUMENTO)


def parece_descarga(href):
    low = href.lower()
    return any(p in low for p in [
        'getdocument', 'getfile', 'download', 'descarga', 'filedownload',
        'servlet', 'adjunto', 'attachment', 'documento', 'filesystem',
        'evidencia',   # contractaciopublica.cat: /evidencia/pdf/{id}
        '/anunci/',    # contractaciopublica.cat: /anunci/...
    ])


def es_navegacion(href):
    low = href.lower()
    return any(p in low for p in _EXCLUIR_NAVEGACION)


def inferir_nombre_fichero(url, texto_enlace, idx):
    path = unquote(urlparse(url).path)
    basename = os.path.basename(path)
    if basename and any(basename.lower().endswith(ext) for ext in EXTENSIONES_DOCUMENTO):
        return basename
    for part in urlparse(url).query.split('&'):
        if '=' in part:
            k, v = part.split('=', 1)
            if k.lower() in ('filename', 'name', 'file', 'doc'):
                v2 = unquote(v)
                if v2:
                    return v2
    if texto_enlace:
        clean = re.sub(r'[<>:"/\\|?*]', '', texto_enlace)[:60].strip()
        if clean:
            # No añadir .pdf si el texto ya termina en extensión conocida
            if any(clean.lower().endswith(ext) for ext in EXTENSIONES_DOCUMENTO):
                return clean
            return clean + '.pdf'
    return f'doc_{idx:02d}.pdf'


def nombre_unico(exp_dir, tipo, filename_base):
    base = f'selenium__{sanitize(tipo)}__{sanitize(filename_base)}'
    dest = exp_dir / base
    if not dest.exists():
        return dest
    stem, suffix = dest.stem, dest.suffix or '.pdf'
    for i in range(1, 100):
        c = exp_dir / f'{stem}__{i:02d}{suffix}'
        if not c.exists():
            return c
    return exp_dir / f'{stem}__x{suffix}'


# ---------------------------------------------------------------------------
# Clasificación de tipo de documento
# ---------------------------------------------------------------------------
MAP_TIPO = {
    'pliego_administrativo': [
        'pcap', 'pliego de clausulas', 'pliego administrativo',
        'condiciones administrativas', 'clausulas administrativas',
        'pliego a.', 'pliego a ', 'pliego_a', 'pliego clausulas',
    ],
    'pliego_tecnico': [
        'ppt', 'prescripciones tecnicas', 'pliego tecnico',
        'especificaciones tecnicas', 'condiciones tecnicas',
        'prescripciones_tecnicas',
    ],
    'anuncio_DOC_CAN_ADJ': [
        'adjudicacion', 'resolucion adjudicacion', 'acuerdo adjudicacion',
        'resolucion de adjudicacion',
    ],
    'anuncio_DOC_CN': [
        'convocatoria', 'anuncio de licitacion', 'boe', 'doue', 'bop', 'bocm',
    ],
    'anuncio_DOC_FORM': [
        'formalizacion', 'contrato formalizado',
    ],
    'anuncio_DOC_MOD': [
        'modificacion', 'modificado',
    ],
}


def clasificar_tipo(contexto, url=''):
    txt = (contexto + ' ' + url).lower()
    txt = txt.replace('á','a').replace('é','e').replace('í','i').replace('ó','o').replace('ú','u')
    for tipo, kws in MAP_TIPO.items():
        if any(kw in txt for kw in kws):
            return tipo
    return 'documento_descargado'


# ---------------------------------------------------------------------------
# Descarga HTTP
# ---------------------------------------------------------------------------
def descargar_binario(url, dest_path, timeout=30, retries=2):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'}
    for attempt in range(retries + 1):
        try:
            r = requests.get(url, timeout=timeout, stream=True, headers=headers)
            if r.status_code == 200:
                ct = r.headers.get('content-type', '').lower()
                if 'text/html' in ct and not url.lower().endswith('.html'):
                    return False, 'Respuesta HTML (no fichero)'
                with open(dest_path, 'wb') as f:
                    for chunk in r.iter_content(8192):
                        f.write(chunk)
                if os.path.getsize(dest_path) < 500:
                    os.remove(dest_path)
                    return False, 'Fichero vacio'
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
                return False, str(e)[:80]
    return False, 'Max reintentos'


def capturar_pagina_pdf(driver, dest_path):
    """Imprime la página actual a PDF via Chrome DevTools Protocol."""
    try:
        driver.execute_script('window.print = function() {};')
        result = driver.execute_cdp_cmd('Page.printToPDF', {
            'landscape': False,
            'printBackground': True,
            'paperWidth': 8.27,
            'paperHeight': 11.69,
            'marginTop': 0.5,
            'marginBottom': 0.5,
            'marginLeft': 0.5,
            'marginRight': 0.5,
        })
        pdf_bytes = base64.b64decode(result['data'])
        if len(pdf_bytes) < 500:
            return False, 'PDF generado vacio'
        with open(dest_path, 'wb') as f:
            f.write(pdf_bytes)
        return True, None
    except Exception as e:
        return False, str(e)[:80]


# ---------------------------------------------------------------------------
# Chrome
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


def aceptar_cookies(driver, timeout=5):
    selectores = [
        "//*[@id='onetrust-accept-btn-handler']",
        "//button[contains(translate(text(),'ACEPTAR','aceptar'),'aceptar')]",
        "//button[contains(translate(text(),'ACEPTO','acepto'),'acepto')]",
        "//*[contains(@class,'cookie') and contains(translate(.,'ACEPTAR','aceptar'),'aceptar')]",
        "//*[contains(@id,'accept')]",
    ]
    for xpath in selectores:
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
# Base de datos
# ---------------------------------------------------------------------------
def get_db():
    conn = sqlite3.connect(RUTA_DB, timeout=30)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.row_factory = sqlite3.Row
    return conn


def get_expedientes(conn, dominio, expediente_filtro=None, limite=None, solo_pendientes=True):
    """
    Devuelve expedientes del dominio indicado que tienen entry_link.
    solo_pendientes=True → solo los que tienen docs sin URL pendientes.
    """
    if solo_pendientes:
        query = '''
            SELECT DISTINCT l.expediente, l.entry_link, l.org_nombre, l.ccaa, l.estado
            FROM licitaciones l
            JOIN documentos d ON d.expediente = l.expediente
            WHERE (d.doc_url IS NULL OR d.doc_url = '') AND d.descargado = 0
              AND l.entry_link LIKE ?
        '''
    else:
        query = '''
            SELECT DISTINCT l.expediente, l.entry_link, l.org_nombre, l.ccaa, l.estado
            FROM licitaciones l
            WHERE l.entry_link LIKE ?
        '''
    params = [f'%{dominio}%']
    if expediente_filtro:
        query += ' AND l.expediente = ?'
        params.append(expediente_filtro)
    if limite:
        query += f' LIMIT {limite}'
    return conn.execute(query, params).fetchall()


def urls_ya_descargadas(conn, expediente):
    return set(
        r[0] for r in conn.execute(
            "SELECT doc_url FROM documentos WHERE expediente=? AND doc_url IS NOT NULL AND doc_url!='' AND descargado=1",
            (expediente,)
        ).fetchall()
    )


def _ruta_relativa(ruta_abs):
    """Convierte ruta absoluta a relativa respecto a RUTA_PDFS para almacenar en BD."""
    try:
        return str(Path(ruta_abs).relative_to(RUTA_PDFS))
    except ValueError:
        return ruta_abs  # fuera de RUTA_PDFS: guardar tal cual


def guardar_doc(conn, expediente, tipo_inf, doc_url, ruta_local, error=None):
    """Guarda o actualiza un documento en la BD de forma incremental.
    ruta_local se almacena como ruta relativa a RUTA_PDFS."""
    if ruta_local:
        # Guardar siempre relativa para independencia de máquina
        ruta_guardada = _ruta_relativa(ruta_local)
        # Intentar actualizar fila existente sin URL del mismo tipo
        fila = conn.execute('''
            SELECT id FROM documentos
            WHERE expediente=? AND tipo_documento=?
              AND (doc_url IS NULL OR doc_url='') AND descargado=0
            LIMIT 1
        ''', (expediente, tipo_inf)).fetchone()
        if fila:
            conn.execute(
                "UPDATE documentos SET descargado=1, ruta_local=?, doc_url=?, error='' WHERE id=?",
                (ruta_guardada, doc_url, fila[0])
            )
        else:
            conn.execute(
                "INSERT INTO documentos (expediente, tipo_documento, doc_url, ruta_local, descargado, error) VALUES (?,?,?,?,1,'')",
                (expediente, tipo_inf, doc_url, ruta_guardada)
            )
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


# ---------------------------------------------------------------------------
# Clase base de portal
# ---------------------------------------------------------------------------
class PortalBase:
    DOMINIO = ''  # sobreescribir en cada subclase

    def __init__(self, driver):
        self.driver = driver
        self._cookies_ok = set()

    def _cookies(self, dominio):
        if dominio not in self._cookies_ok:
            if aceptar_cookies(self.driver, timeout=4):
                print(f'    [cookies OK]')
            self._cookies_ok.add(dominio)

    def navegar(self, url):
        self.driver.get(url)
        time.sleep(2)
        self._cookies(get_domain(url))

    def _contexto(self, tag):
        for p in ['tr', 'li', 'div', 'td', 'p']:
            parent = tag.find_parent(p)
            if parent:
                txt = parent.get_text(separator=' ', strip=True)
                if len(txt) > 5:
                    return txt[:300]
        return ''

    def encontrar_enlaces(self, base_url):
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        base_domain = get_domain(base_url)
        encontrados, vistos = [], set()
        for tag in soup.find_all('a', href=True):
            href = tag['href'].strip()
            if not href or href.startswith('#'):
                continue
            href_abs = urljoin(base_url, href)
            p = urlparse(href_abs)
            if p.netloc and p.netloc != base_domain:
                continue
            if not (ext_valida(href_abs) or parece_descarga(href_abs)):
                continue
            if es_navegacion(href_abs):
                continue
            if href_abs in vistos:
                continue
            vistos.add(href_abs)
            texto = tag.get_text(strip=True)
            ctx = self._contexto(tag)
            tipo = clasificar_tipo(ctx + ' ' + texto, href_abs)
            encontrados.append({'url': href_abs, 'texto': texto, 'ctx': ctx, 'tipo': tipo})
        return encontrados

    def descargar_enlaces(self, enlaces, exp_dir, urls_descargadas):
        resultados = []
        for idx, e in enumerate(enlaces):
            if e['url'] in urls_descargadas:
                print(f'      [ya existe] {e["url"][:70]}')
                continue
            filename = inferir_nombre_fichero(e['url'], e['texto'], idx)
            dest = nombre_unico(exp_dir, e['tipo'], filename)
            ok, err = descargar_binario(e['url'], str(dest))
            if ok:
                print(f'      OK  {e["tipo"]:<28} {dest.name}')
                resultados.append((str(dest), e['tipo'], e['url'], None))
            else:
                print(f'      ERR {e["tipo"]:<28} {err}')
                resultados.append((None, e['tipo'], e['url'], err))
        return resultados

    def procesar(self, entry_link, expediente, exp_dir, urls_descargadas):
        self.navegar(entry_link)
        enlaces = self.encontrar_enlaces(entry_link)
        return self.descargar_enlaces(enlaces, exp_dir, urls_descargadas)


# ---------------------------------------------------------------------------
# Bucle de ejecución común (reutilizable por cada scraper de portal)
# ---------------------------------------------------------------------------
def ejecutar_portal(portal_cls, dominio_filtro, args):
    """
    Función reutilizable por cada scraper individual.
    portal_cls: clase del portal (PortalBase o subclase)
    dominio_filtro: string para filtrar entry_link (ej: 'euskadi.eus')
    args: namespace de argparse
    """
    conn = get_db()

    if hasattr(args, 'stats') and args.stats:
        _mostrar_stats_portal(conn, dominio_filtro)
        conn.close()
        return

    exptes = get_expedientes(
        conn, dominio_filtro,
        expediente_filtro=getattr(args, 'expediente', None),
        limite=getattr(args, 'limite', None),
        solo_pendientes=not getattr(args, 'todos', False),
    )

    print(f'\nPortal: {dominio_filtro}')
    print(f'Expedientes: {len(exptes)}')
    if not exptes:
        print('Nada pendiente.')
        conn.close()
        return

    driver = crear_driver(headless=getattr(args, 'headless', False))
    handler = portal_cls(driver)
    ok = err = nuevos = 0
    delay = getattr(args, 'delay', 2)

    try:
        for i, row in enumerate(exptes, 1):
            expediente = row['expediente']
            entry_link = row['entry_link']
            print(f'\n[{i}/{len(exptes)}] {expediente} | {row["ccaa"] or "?"} | {row["estado"] or "?"}')
            print(f'  {(row["org_nombre"] or "")[:70]}')

            exp_dir = Path(RUTA_PDFS) / sanitize(expediente)
            exp_dir.mkdir(parents=True, exist_ok=True)
            ya_desc = urls_ya_descargadas(conn, expediente)

            try:
                resultados = handler.procesar(entry_link, expediente, exp_dir, ya_desc)
                n = sum(1 for r in resultados if r[0])
                nuevos += n
                for ruta, tipo, url, error in resultados:
                    guardar_doc(conn, expediente, tipo, url, ruta, error)
                conn.commit()
                ok += 1
                print(f'  --> {n} nuevos')
            except Exception as e:
                msg = str(e)
                if 'invalid session' in msg.lower() or 'no such session' in msg.lower():
                    print(f'  [Chrome caido] Reiniciando...')
                    try: driver.quit()
                    except: pass
                    time.sleep(2)
                    driver = crear_driver(headless=getattr(args, 'headless', False))
                    handler = portal_cls(driver)
                    err += 1
                else:
                    print(f'  [ERROR] {msg[:100]}')
                    err += 1
            time.sleep(delay)
    finally:
        try: driver.quit()
        except: pass

    conn.close()
    print(f'\n--- {dominio_filtro} ---')
    print(f'OK: {ok} | ERR: {err} | Docs nuevos: {nuevos}')


def _mostrar_stats_portal(conn, dominio):
    rows = conn.execute('''
        SELECT d.tipo_documento,
               COUNT(*) total,
               SUM(CASE WHEN d.descargado=1 THEN 1 ELSE 0 END) desc_ok
        FROM documentos d
        JOIN licitaciones l ON l.expediente=d.expediente
        WHERE l.entry_link LIKE ?
        GROUP BY d.tipo_documento ORDER BY total DESC
    ''', (f'%{dominio}%',)).fetchall()
    exptes = conn.execute(
        "SELECT COUNT(DISTINCT expediente) FROM licitaciones WHERE entry_link LIKE ?",
        (f'%{dominio}%',)
    ).fetchone()[0]
    pend = conn.execute('''
        SELECT COUNT(*) FROM documentos d
        JOIN licitaciones l ON l.expediente=d.expediente
        WHERE l.entry_link LIKE ? AND d.descargado=0 AND (d.doc_url IS NULL OR d.doc_url='')
    ''', (f'%{dominio}%',)).fetchone()[0]
    print(f'\n=== {dominio} | {exptes} expedientes | {pend} pendientes ===')
    print(f'  {"Tipo":<32} {"Total":>6} {"Desc":>6}')
    for r in rows:
        print(f'  {r[0]:<32} {r[1]:>6} {r[2] or 0:>6}')


def args_comunes(parser):
    """Añade argumentos estándar a un argparse."""
    parser.add_argument('--expediente', default=None)
    parser.add_argument('--limite', type=int, default=None)
    parser.add_argument('--todos', action='store_true',
                        help='Procesar todos (no solo pendientes)')
    parser.add_argument('--headless', action='store_true')
    parser.add_argument('--delay', type=float, default=2.0)
    parser.add_argument('--stats', action='store_true')
    return parser
