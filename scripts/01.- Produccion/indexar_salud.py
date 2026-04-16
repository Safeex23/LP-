"""
indexar_salud.py — Carga los 4 CSVs salud en licitaciones_salud.db
===================================================================
Lee los CSVs de salud/datos/csv/{anio}/ y los indexa en SQLite.
Safe para re-ejecutar: INSERT OR REPLACE en licitaciones, DELETE+INSERT en tablas relacionadas.

Uso:
    python salud/scripts/indexar_salud.py            # año actual
    python salud/scripts/indexar_salud.py 2026       # año concreto
    python salud/scripts/indexar_salud.py --stats    # solo estadísticas
"""

import sys
import os
import math
import sqlite3
from datetime import datetime

import pandas as pd

BASE_SALUD = r'C:\proyectos\licitaciones\salud\datos'
RUTA_DB    = r'C:\proyectos\licitaciones\salud\datos\licitaciones_salud.db'


# ---------------------------------------------------------------------------
# Helpers de tipo
# ---------------------------------------------------------------------------
def _is_empty(v):
    if v is None:
        return True
    try:
        if math.isnan(float(v)):
            return True
    except (TypeError, ValueError):
        pass
    return str(v).strip() in ('', 'nan', 'None', 'Sin dato')


def _to_str(v):
    return '' if _is_empty(v) else str(v).strip()


def _to_float(v):
    if _is_empty(v):
        return None
    try:
        return float(str(v).replace(',', '.'))
    except (ValueError, TypeError):
        return None


def _to_int(v, default=0):
    try:
        f = float(str(v).strip())
        return int(f)
    except (ValueError, TypeError):
        return default


# ---------------------------------------------------------------------------
# Esquema
# ---------------------------------------------------------------------------
def crear_db(conn):
    conn.execute('PRAGMA journal_mode=WAL')

    conn.execute('''
        CREATE TABLE IF NOT EXISTS licitaciones (
            expediente                  TEXT PRIMARY KEY,
            fuente                      TEXT,
            periodo                     TEXT,
            fecha_carga                 TEXT,
            estado                      TEXT,
            estado_code                 TEXT,
            titulo                      TEXT,
            objeto_contrato             TEXT,
            org_nombre                  TEXT,
            org_nif                     TEXT,
            org_dir3                    TEXT,
            org_ciudad                  TEXT,
            org_provincia               TEXT,
            org_pais                    TEXT,
            ccaa                        TEXT,
            lugar_ejecucion_nuts        TEXT,
            procedimiento               TEXT,
            procedimiento_code          TEXT,
            tramitacion                 TEXT,
            urgencia_code               TEXT,
            tipo_contrato_code          TEXT,
            subtipo_contrato_code       TEXT,
            presupuesto_estimado        REAL,
            importe_licitacion_sin_iva  REAL,
            importe_licitacion_con_iva  REAL,
            cpv_principal               TEXT,
            cpvs_adicionales            TEXT,
            duracion_contrato           TEXT,
            duracion_unidad             TEXT,
            financiacion                TEXT,
            programa_financiacion_code  TEXT,
            solvencia_economica         TEXT,
            solvencia_tecnica           TEXT,
            garantia_tipo               TEXT,
            garantia_porcentaje         TEXT,
            condiciones_ejecucion       TEXT,
            subcontratacion_permitida   TEXT,
            fecha_limite_presentacion   TEXT,
            hora_limite_presentacion    TEXT,
            num_lotes                   INTEGER DEFAULT 0,
            num_documentos              INTEGER DEFAULT 0,
            adjudicatario_nombre        TEXT,
            adjudicatario_nif           TEXT,
            importe_adjudicacion        REAL,
            fecha_adjudicacion          TEXT,
            num_ofertas_recibidas       TEXT,
            fecha_formalizacion         TEXT,
            entry_link                  TEXT,
            buyer_profile               TEXT
        )
    ''')

    conn.execute('''
        CREATE TABLE IF NOT EXISTS lotes (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            expediente            TEXT,
            lote_id               TEXT,
            objeto_lote           TEXT,
            importe_lote          REAL,
            cpv_lote              TEXT,
            adjudicatario_nombre  TEXT,
            importe_adjudicacion  REAL,
            FOREIGN KEY (expediente) REFERENCES licitaciones(expediente)
        )
    ''')

    conn.execute('''
        CREATE TABLE IF NOT EXISTS criterios (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            expediente    TEXT,
            lote_id       TEXT,
            tipo_criterio TEXT,
            descripcion   TEXT,
            peso          TEXT,
            FOREIGN KEY (expediente) REFERENCES licitaciones(expediente)
        )
    ''')

    conn.execute('''
        CREATE TABLE IF NOT EXISTS documentos (
            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            expediente         TEXT,
            tipo_documento     TEXT,
            doc_id             TEXT,
            doc_url            TEXT,
            doc_filename       TEXT,
            medio_publicacion  TEXT,
            fecha_publicacion  TEXT,
            ruta_local         TEXT DEFAULT '',
            descargado         INTEGER DEFAULT 0,
            error              TEXT DEFAULT '',
            FOREIGN KEY (expediente) REFERENCES licitaciones(expediente)
        )
    ''')

    # Historial de cambios de estado — nunca se borra, solo se añade
    conn.execute('''
        CREATE TABLE IF NOT EXISTS historial_estados (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            expediente       TEXT NOT NULL,
            estado_anterior  TEXT,
            estado_nuevo     TEXT NOT NULL,
            periodo_deteccion TEXT,
            fecha_deteccion  TEXT NOT NULL,
            FOREIGN KEY (expediente) REFERENCES licitaciones(expediente)
        )
    ''')

    # Índices
    conn.execute('CREATE INDEX IF NOT EXISTS idx_ccaa        ON licitaciones(ccaa)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_estado      ON licitaciones(estado)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_fuente      ON licitaciones(fuente)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_periodo     ON licitaciones(periodo)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_cpv         ON licitaciones(cpv_principal)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_doc_exp     ON documentos(expediente)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_doc_desc    ON documentos(descargado)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_hist_exp    ON historial_estados(expediente)')
    conn.commit()
    print('  Esquema licitaciones_salud.db creado/verificado.')


# ---------------------------------------------------------------------------
# Columnas de licitaciones
# ---------------------------------------------------------------------------
COLS_LIC = [
    'expediente', 'fuente', 'periodo', 'fecha_carga', 'estado', 'estado_code',
    'titulo', 'objeto_contrato',
    'org_nombre', 'org_nif', 'org_dir3', 'org_ciudad', 'org_provincia', 'org_pais',
    'ccaa', 'lugar_ejecucion_nuts',
    'procedimiento', 'procedimiento_code', 'tramitacion', 'urgencia_code',
    'tipo_contrato_code', 'subtipo_contrato_code',
    'presupuesto_estimado', 'importe_licitacion_sin_iva', 'importe_licitacion_con_iva',
    'cpv_principal', 'cpvs_adicionales',
    'duracion_contrato', 'duracion_unidad',
    'financiacion', 'programa_financiacion_code',
    'solvencia_economica', 'solvencia_tecnica',
    'garantia_tipo', 'garantia_porcentaje',
    'condiciones_ejecucion', 'subcontratacion_permitida',
    'fecha_limite_presentacion', 'hora_limite_presentacion',
    'num_lotes', 'num_documentos',
    'adjudicatario_nombre', 'adjudicatario_nif',
    'importe_adjudicacion', 'fecha_adjudicacion',
    'num_ofertas_recibidas', 'fecha_formalizacion',
    'entry_link', 'buyer_profile',
]
COLS_FLOAT = {'presupuesto_estimado', 'importe_licitacion_sin_iva',
              'importe_licitacion_con_iva', 'importe_adjudicacion'}
COLS_INT   = {'num_lotes', 'num_documentos'}


def _ruta_csv(anio, nombre):
    return os.path.join(BASE_SALUD, 'csv', anio, f'{nombre}_salud_{anio}.csv')


# ---------------------------------------------------------------------------
# Indexación
# ---------------------------------------------------------------------------
def indexar_todo(conn, anio):
    ruta_lics = _ruta_csv(anio, 'licitaciones')
    if not os.path.exists(ruta_lics):
        print(f'  ERROR: no existe {ruta_lics}')
        print(f'  Ejecuta primero: python salud/scripts/etl_salud.py {anio}')
        return

    # ---- licitaciones (con detección de cambios de estado) ----
    df = pd.read_csv(ruta_lics, sep=';', encoding='utf-8-sig', dtype={'expediente': str})
    print(f'  Indexando {len(df)} licitaciones...')
    ph = ','.join(['?'] * len(COLS_LIC))
    ahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cambios_estado = 0

    for _, row in df.iterrows():
        expediente = _to_str(row.get('expediente', ''))
        estado_nuevo = _to_str(row.get('estado', ''))
        periodo_actual = _to_str(row.get('periodo', anio))

        # Comprobar estado anterior antes de hacer el REPLACE
        fila_actual = conn.execute(
            'SELECT estado FROM licitaciones WHERE expediente=?', (expediente,)
        ).fetchone()

        if fila_actual is not None:
            estado_anterior = fila_actual[0] or ''
            if estado_anterior and estado_anterior != estado_nuevo:
                # Hay cambio de estado — registrar en historial
                conn.execute('''
                    INSERT INTO historial_estados
                    (expediente, estado_anterior, estado_nuevo, periodo_deteccion, fecha_deteccion)
                    VALUES (?,?,?,?,?)
                ''', (expediente, estado_anterior, estado_nuevo, periodo_actual, ahora))
                cambios_estado += 1
        else:
            # Primera vez que se indexa este expediente — registrar estado inicial
            if estado_nuevo:
                conn.execute('''
                    INSERT INTO historial_estados
                    (expediente, estado_anterior, estado_nuevo, periodo_deteccion, fecha_deteccion)
                    VALUES (?,?,?,?,?)
                ''', (expediente, None, estado_nuevo, periodo_actual, ahora))

        vals = []
        for c in COLS_LIC:
            v = row.get(c, '')
            if c in COLS_FLOAT:
                vals.append(_to_float(v))
            elif c in COLS_INT:
                vals.append(_to_int(v))
            else:
                vals.append(_to_str(v))
        conn.execute(
            f'INSERT OR REPLACE INTO licitaciones ({",".join(COLS_LIC)}) VALUES ({ph})',
            vals,
        )

    conn.commit()
    print(f'  Licitaciones indexadas: {len(df)}')
    if cambios_estado:
        print(f'  Cambios de estado detectados: {cambios_estado} (guardados en historial_estados)')

    expedientes = df['expediente'].astype(str).tolist()

    def _limpiar_e_insertar(tabla, ruta_csv, fn_insert):
        if not os.path.exists(ruta_csv):
            print(f'  AVISO: no existe {ruta_csv}')
            return 0
        df_t = pd.read_csv(ruta_csv, sep=';', encoding='utf-8-sig', dtype={'expediente': str})
        exps = df_t['expediente'].astype(str).unique().tolist()
        if exps:
            ph_e = ','.join(['?'] * len(exps))
            conn.execute(f'DELETE FROM {tabla} WHERE expediente IN ({ph_e})', exps)
        for _, row in df_t.iterrows():
            fn_insert(conn, row)
        conn.commit()
        print(f'  {tabla.capitalize()} indexados: {len(df_t)}')
        return len(df_t)

    # ---- lotes ----
    def ins_lote(conn, row):
        conn.execute(
            'INSERT INTO lotes (expediente, lote_id, objeto_lote, importe_lote, cpv_lote, adjudicatario_nombre, importe_adjudicacion) VALUES (?,?,?,?,?,?,?)',
            (
                _to_str(row.get('expediente', '')),
                _to_str(row.get('lote_id', '')),
                _to_str(row.get('objeto_lote', '')),
                _to_float(row.get('importe_lote', '')),
                _to_str(row.get('cpv_lote', '')),
                _to_str(row.get('adjudicatario_nombre', '')),
                _to_float(row.get('importe_adjudicacion', '')),
            ),
        )
    _limpiar_e_insertar('lotes', _ruta_csv(anio, 'lotes'), ins_lote)

    # ---- criterios ----
    def ins_criterio(conn, row):
        conn.execute(
            'INSERT INTO criterios (expediente, lote_id, tipo_criterio, descripcion, peso) VALUES (?,?,?,?,?)',
            (
                _to_str(row.get('expediente', '')),
                _to_str(row.get('lote_id', '')),
                _to_str(row.get('tipo_criterio', '')),
                _to_str(row.get('descripcion', '')),
                _to_str(row.get('peso', '')),
            ),
        )
    _limpiar_e_insertar('criterios', _ruta_csv(anio, 'criterios'), ins_criterio)

    # ---- documentos ----
    def ins_doc(conn, row):
        conn.execute(
            '''INSERT INTO documentos
               (expediente, tipo_documento, doc_id, doc_url, doc_filename,
                medio_publicacion, fecha_publicacion, ruta_local, descargado, error)
               VALUES (?,?,?,?,?,?,?,?,?,?)''',
            (
                _to_str(row.get('expediente', '')),
                _to_str(row.get('tipo_documento', '')),
                _to_str(row.get('doc_id', '')),
                _to_str(row.get('doc_url', '')),
                _to_str(row.get('doc_filename', '')),
                _to_str(row.get('medio_publicacion', '')),
                _to_str(row.get('fecha_publicacion', '')),
                _to_str(row.get('ruta_local', '')),
                _to_int(row.get('descargado', 0)),
                _to_str(row.get('error', '')),
            ),
        )
    _limpiar_e_insertar('documentos', _ruta_csv(anio, 'documentos'), ins_doc)


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------
def mostrar_stats(conn):
    print('\n=== STATS licitaciones_salud.db ===')

    total = conn.execute('SELECT COUNT(*) FROM licitaciones').fetchone()[0]
    print(f'  Total licitaciones : {total}')

    print('\n  Por CPV:')
    for cpv, cnt in conn.execute(
        "SELECT cpv_principal, COUNT(*) c FROM licitaciones GROUP BY cpv_principal ORDER BY c DESC LIMIT 15"
    ).fetchall():
        print(f'    {cpv or "(vacío)"}: {cnt}')

    print('\n  Por CCAA (top 10):')
    for ccaa, cnt in conn.execute(
        "SELECT ccaa, COUNT(*) c FROM licitaciones GROUP BY ccaa ORDER BY c DESC LIMIT 10"
    ).fetchall():
        print(f'    {ccaa or "(vacío)"}: {cnt}')

    print('\n  Por estado:')
    for est, cnt in conn.execute(
        "SELECT estado, COUNT(*) c FROM licitaciones GROUP BY estado ORDER BY c DESC"
    ).fetchall():
        print(f'    {est or "(vacío)"}: {cnt}')

    print('\n  Por fuente:')
    for f, cnt in conn.execute(
        "SELECT fuente, COUNT(*) c FROM licitaciones GROUP BY fuente ORDER BY c DESC"
    ).fetchall():
        print(f'    {f or "(vacío)"}: {cnt}')

    tot_docs = conn.execute('SELECT COUNT(*) FROM documentos').fetchone()[0]
    desc = conn.execute('SELECT COUNT(*) FROM documentos WHERE descargado=1').fetchone()[0]
    con_url = conn.execute("SELECT COUNT(*) FROM documentos WHERE doc_url != '' AND doc_url IS NOT NULL").fetchone()[0]
    print(f'\n  Documentos totales  : {tot_docs}')
    print(f'  Con URL directa     : {con_url}')
    print(f'  Descargados         : {desc}')

    tot_lotes = conn.execute('SELECT COUNT(*) FROM lotes').fetchone()[0]
    print(f'\n  Lotes               : {tot_lotes}')

    importes = conn.execute(
        'SELECT SUM(importe_licitacion_sin_iva), AVG(importe_licitacion_sin_iva), MAX(importe_licitacion_sin_iva) FROM licitaciones WHERE importe_licitacion_sin_iva > 0'
    ).fetchone()
    if importes[0]:
        print(f'\n  Importe total       : {importes[0]:,.0f} EUR')
        print(f'  Importe medio       : {importes[1]:,.0f} EUR')
        print(f'  Importe max         : {importes[2]:,.0f} EUR')


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Indexar CSVs salud en SQLite')
    parser.add_argument('anio', nargs='?', default=str(datetime.now().year))
    parser.add_argument('--stats', action='store_true', help='Solo mostrar estadísticas')
    args = parser.parse_args()

    os.makedirs(os.path.dirname(RUTA_DB), exist_ok=True)
    conn = sqlite3.connect(RUTA_DB, timeout=30)

    if args.stats:
        mostrar_stats(conn)
        conn.close()
        sys.exit(0)

    print(f'\n=== INDEXAR SALUD | Año {args.anio} ===')
    crear_db(conn)
    indexar_todo(conn, args.anio)

    print('\n--- Verificación final ---')
    mostrar_stats(conn)
    conn.close()
    print(f'\nBD: {RUTA_DB}')
