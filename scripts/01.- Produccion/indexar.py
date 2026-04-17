# indexar.py - Indexacion SQLite desde los 4 CSVs anuales (arquitectura parse_placsp)

import pandas as pd
import sqlite3
import os
import math
from datetime import datetime

BASE_OD   = r'C:\Users\sfenoll\OneDrive - INTEGRA\Documentos\03.- Licitaciones\Maestro\csv'
RUTA_DB   = r'C:\proyectos\licitaciones\datos\Repositorio\licitaciones.db'
RUTA_PDFS = r'C:\proyectos\licitaciones\datos\Repositorio\pdfs'


def _schema_es_nuevo(conn):
    """Devuelve True si licitaciones ya tiene el esquema nuevo (columna org_nombre)."""
    cols = [r[1] for r in conn.execute('PRAGMA table_info(licitaciones)').fetchall()]
    return 'org_nombre' in cols


def crear_db(conn):
    # Si existe la tabla con el esquema viejo, la borramos para migrar al nuevo
    tablas = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    if 'licitaciones' in tablas and not _schema_es_nuevo(conn):
        print('  Detectado esquema antiguo. Eliminando tablas para migrar al nuevo esquema...')
        for t in ['criterios', 'documentos', 'lotes', 'licitaciones']:
            conn.execute(f'DROP TABLE IF EXISTS {t}')
        conn.commit()
        print('  Tablas antiguas eliminadas.')

    conn.execute('''
        CREATE TABLE IF NOT EXISTS licitaciones (
            expediente TEXT PRIMARY KEY,
            fuente TEXT, periodo TEXT, fecha_carga TEXT,
            estado TEXT, estado_code TEXT,
            titulo TEXT, objeto_contrato TEXT,
            org_nombre TEXT, org_nif TEXT, org_dir3 TEXT,
            org_ciudad TEXT, org_provincia TEXT, org_pais TEXT,
            ccaa TEXT, lugar_ejecucion_nuts TEXT,
            procedimiento TEXT, procedimiento_code TEXT,
            tramitacion TEXT, urgencia_code TEXT,
            tipo_contrato_code TEXT, subtipo_contrato_code TEXT,
            presupuesto_estimado REAL, importe_licitacion_sin_iva REAL, importe_licitacion_con_iva REAL,
            cpv_principal TEXT, cpvs_adicionales TEXT,
            duracion_contrato TEXT, duracion_unidad TEXT,
            financiacion TEXT, programa_financiacion_code TEXT,
            solvencia_economica TEXT, solvencia_tecnica TEXT,
            garantia_tipo TEXT, garantia_porcentaje TEXT,
            condiciones_ejecucion TEXT, subcontratacion_permitida TEXT,
            fecha_limite_presentacion TEXT, hora_limite_presentacion TEXT,
            num_lotes INTEGER DEFAULT 0, num_documentos INTEGER DEFAULT 0,
            adjudicatario_nombre TEXT, adjudicatario_nif TEXT,
            importe_adjudicacion REAL, fecha_adjudicacion TEXT,
            num_ofertas_recibidas TEXT, fecha_formalizacion TEXT,
            entry_link TEXT, buyer_profile TEXT
        )
    ''')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS lotes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            expediente TEXT, lote_id TEXT,
            objeto_lote TEXT, importe_lote REAL, cpv_lote TEXT,
            adjudicatario_nombre TEXT, importe_adjudicacion REAL,
            FOREIGN KEY (expediente) REFERENCES licitaciones(expediente)
        )
    ''')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS criterios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            expediente TEXT, lote_id TEXT,
            tipo_criterio TEXT, descripcion TEXT, peso TEXT,
            FOREIGN KEY (expediente) REFERENCES licitaciones(expediente)
        )
    ''')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS documentos (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            expediente TEXT, tipo_documento TEXT,
            doc_id TEXT, doc_url TEXT, doc_filename TEXT,
            medio_publicacion TEXT, fecha_publicacion TEXT,
            ruta_local TEXT DEFAULT '', descargado INTEGER DEFAULT 0, error TEXT DEFAULT '',
            FOREIGN KEY (expediente) REFERENCES licitaciones(expediente)
        )
    ''')
    # Índice único para UPSERT seguro: preserva ruta_local/descargado al re-indexar
    conn.execute('''
        CREATE UNIQUE INDEX IF NOT EXISTS idx_doc_unico
        ON documentos(expediente, doc_id)
        WHERE doc_id IS NOT NULL AND TRIM(doc_id) != ''
    ''')
    # conn.execute('CREATE INDEX IF NOT EXISTS idx_ccaa    ON licitaciones(ccaa)')
    # Index on 'estado' removed as it may have low cardinality and limited benefit.
    # conn.execute('CREATE INDEX IF NOT EXISTS idx_fuente  ON licitaciones(fuente)')
    # conn.execute('CREATE INDEX IF NOT EXISTS idx_periodo ON licitaciones(periodo)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_periodo ON licitaciones(periodo)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_cpv     ON licitaciones(cpv_principal)')
    # Index on 'expediente' in documentos: useful for join operations with licitaciones.
    conn.execute('CREATE INDEX IF NOT EXISTS idx_doc_exp ON documentos(expediente)')
    conn.commit()
    print('  Base de datos creada/verificada.')
    conn.commit()
    print('  Base de datos creada/verificada.')


def _ruta_csv(anio, nombre):
    return os.path.join(BASE_OD, anio, f'{nombre}_{anio}.csv')


def _is_empty(v):
    """True si el valor es NaN, None o cadena vacía/Sin dato."""
    if v is None:
        return True
    try:
        if math.isnan(v):
            return True
    except (TypeError, ValueError):
        pass
    return str(v).strip() in ('', 'nan', 'Sin dato')


def _to_str(v):
    return '' if _is_empty(v) else str(v).strip()


def _to_float(v):
    if _is_empty(v):
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _to_int(v, default=0):
    try:
        return int(float(v)) if str(v).strip() not in ('', 'Sin dato') else default
    except (ValueError, TypeError):
        return default


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
COLS_FLOAT = {'presupuesto_estimado', 'importe_licitacion_sin_iva', 'importe_licitacion_con_iva', 'importe_adjudicacion'}
COLS_INT   = {'num_lotes', 'num_documentos'}


def indexar_todo(conn, anio):
    """Lee los 4 CSVs anuales desde OneDrive y hace INSERT OR REPLACE en las 4 tablas."""

    # --- licitaciones ---
    ruta = _ruta_csv(anio, 'licitaciones')
    if not os.path.exists(ruta):
        print(f'  AVISO: no existe {ruta}')
        return
    df = pd.read_csv(ruta, sep=';', encoding='utf-8-sig', dtype={'expediente': str}, low_memory=False)
    print(f'  Indexando {len(df)} licitaciones...')
    placeholders = ','.join(['?'] * len(COLS_LIC))
    for _, row in df.iterrows():
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
            f'INSERT OR REPLACE INTO licitaciones ({",".join(COLS_LIC)}) VALUES ({placeholders})',
            vals,
        )
    conn.commit()
    print(f'  Licitaciones indexadas: {len(df)}')

    def _limpiar_y_reinsertar(tabla, ruta_csv, insertar_fn):
        """Para lotes y criterios: DELETE + INSERT (no tienen estado local)."""
        if not os.path.exists(ruta_csv):
            return
        df_t = pd.read_csv(ruta_csv, sep=';', encoding='utf-8-sig', dtype={'expediente': str})
        exps = df_t['expediente'].astype(str).unique().tolist()
        if exps:
            ph = ','.join(['?'] * len(exps))
            conn.execute(f'DELETE FROM {tabla} WHERE expediente IN ({ph})', exps)
        for _, row in df_t.iterrows():
            insertar_fn(conn, row)
        conn.commit()
        print(f'  {tabla.capitalize()} indexados: {len(df_t)}')

    # --- lotes ---
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
    _limpiar_y_reinsertar('lotes', _ruta_csv(anio, 'lotes'), ins_lote)

    # --- criterios ---
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
    _limpiar_y_reinsertar('criterios', _ruta_csv(anio, 'criterios'), ins_criterio)

    # --- documentos: UPSERT — preserva ruta_local/descargado (estado Selenium) ---
    def _upsert_documentos(ruta_csv):
        """
        Para documentos del ATOM feed: UPSERT por (expediente, doc_id).
        - Actualiza metadatos públicos (url, tipo, fecha...)
        - NO sobreescribe ruta_local ni descargado (estado de descarga local)
        Filas sin doc_id (añadidas por Selenium): INSERT ignorando conflicto.
        """
        if not os.path.exists(ruta_csv):
            return
        df_t = pd.read_csv(ruta_csv, sep=';', encoding='utf-8-sig', dtype={'expediente': str}, low_memory=False)
        n = 0
        for _, row in df_t.iterrows():
            expediente   = _to_str(row.get('expediente', ''))
            tipo         = _to_str(row.get('tipo_documento', ''))
            doc_id       = _to_str(row.get('doc_id', ''))
            doc_url      = _to_str(row.get('doc_url', ''))
            doc_filename = _to_str(row.get('doc_filename', ''))
            medio        = _to_str(row.get('medio_publicacion', ''))
            fecha_pub    = _to_str(row.get('fecha_publicacion', ''))
            # ruta_local y descargado del CSV solo se usan si no existe fila previa
            ruta_local   = _to_str(row.get('ruta_local', ''))
            descargado   = _to_int(row.get('descargado', 0))
            error        = _to_str(row.get('error', ''))

            if doc_id:
                # UPSERT: actualiza metadatos pero preserva estado de descarga local
                conn.execute('''
                    INSERT INTO documentos
                        (expediente, tipo_documento, doc_id, doc_url, doc_filename,
                         medio_publicacion, fecha_publicacion, ruta_local, descargado, error)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(expediente, doc_id) DO UPDATE SET
                        tipo_documento   = excluded.tipo_documento,
                        doc_url          = excluded.doc_url,
                        doc_filename     = excluded.doc_filename,
                        medio_publicacion= excluded.medio_publicacion,
                        fecha_publicacion= excluded.fecha_publicacion
                        -- ruta_local, descargado, error: NO se tocan
                ''', (expediente, tipo, doc_id, doc_url, doc_filename,
                      medio, fecha_pub, ruta_local, descargado, error))
            else:
                # Sin doc_id: INSERT OR IGNORE (probablemente ya existe)
                conn.execute('''
                    INSERT OR IGNORE INTO documentos
                        (expediente, tipo_documento, doc_id, doc_url, doc_filename,
                         medio_publicacion, fecha_publicacion, ruta_local, descargado, error)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                ''', (expediente, tipo, doc_id, doc_url, doc_filename,
                      medio, fecha_pub, ruta_local, descargado, error))
            n += 1
        conn.commit()
        print(f'  Documentos indexados (upsert): {n}')

    _upsert_documentos(_ruta_csv(anio, 'documentos'))


if __name__ == '__main__':
    os.makedirs(os.path.dirname(RUTA_DB), exist_ok=True)
    conn = sqlite3.connect(RUTA_DB, timeout=30)
    crear_db(conn)
    anio = str(datetime.now().year)
    print(f'Indexando año {anio}...')
    indexar_todo(conn, anio)
    total = conn.execute('SELECT COUNT(*) FROM licitaciones').fetchone()[0]
    total_docs = conn.execute('SELECT COUNT(*) FROM documentos').fetchone()[0]
    conn.close()
    print(f'Total licitaciones en BD: {total}')
    print(f'Total documentos en BD:   {total_docs}')
