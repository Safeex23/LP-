"""
etl_salud.py — Pipeline ETL para licitaciones del sector salud (CPV 33* y 85*)
===============================================================================
Pipeline paralelo al general. No modifica scripts existentes.
Reutiliza parse_placsp.py como librería.

- Fuentes: Sector Publico + Agregacion + Contratos Menores (sindicación 1143)
- Filtro: CPV principal O adicional que empiece por '33' o '85'
- Datos de entrada: ZIPs/atoms ya descargados en datos\Sector Publico\raw y datos\Agregacion\raw
- Salida: 4 CSVs acumulados en salud\datos\csv\{anio}\
- NO descarga nada nuevo (usa atoms existentes)

Uso:
    # Todos los meses de 2026 disponibles
    python salud/scripts/etl_salud.py

    # Solo un mes concreto
    python salud/scripts/etl_salud.py 2026 03

    # Ver stats tras ejecutar
    python salud/scripts/etl_salud.py --stats
"""

import sys
import os
import csv
import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd

# Apuntar a scripts/01.- Produccion del proyecto principal para importar parse_placsp
_SCRIPTS = os.path.normpath(os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    '..', '..', '..', 'scripts', '01.- Produccion'
))
sys.path.insert(0, _SCRIPTS)
import parse_placsp as pp

# ---------------------------------------------------------------------------
# Rutas
# ---------------------------------------------------------------------------
BASE_DATOS   = r'C:\proyectos\licitaciones\datos'        # atoms originales
BASE_SALUD   = r'C:\proyectos\licitaciones\salud\datos'  # salida CSV

FUENTES_SALUD = [
    ('Sector Publico',    'licitacionesPerfilesContratanteCompleto3.atom'),
    ('Agregacion',        'PlataformasAgregadasSinMenores.atom'),
    ('Contratos Menores', 'contratosMenoresPerfilesContratantes.atom'),  # sindicación 1143
]
CARPETA_FUENTE = {
    'Sector Publico':    'Sector Publico',
    'Agregacion':        'Agregacion',
    'Contratos Menores': 'Menores',
}

# CPVs de salud / sanidad / servicios sociales
CPV_PREFIJOS_SALUD = ('33', '85')

# Meses disponibles en 2026 (detectados automáticamente, pero se puede sobrescribir)
MESES_DEFAULT_2026 = ['01', '02', '03', '04']


# ---------------------------------------------------------------------------
# Filtrado CPV
# ---------------------------------------------------------------------------
def es_cpv_salud(cpv_principal, cpvs_adicionales=''):
    """
    Devuelve True si el CPV principal O alguno de los adicionales pertenece a 33* o 85*.
    cpvs_adicionales: cadena separada por comas/espacios con los CPVs extra.
    """
    todos = [str(cpv_principal or '').strip()]
    if cpvs_adicionales:
        todos += [c.strip() for c in str(cpvs_adicionales).replace(',', ' ').split()]
    return any(c.startswith(p) for c in todos for p in CPV_PREFIJOS_SALUD if c)


def filtrar_salud(lics, lotes, criterios, docs):
    """
    Aplica filtro CPV 33*/85* sobre la lista de licitaciones.
    Evalúa cpv_principal Y cpvs_adicionales para no perder expedientes mixtos.
    Los lotes, criterios y documentos se filtran por los expedientes supervivientes.
    """
    lics_salud = [
        l for l in lics
        if es_cpv_salud(l.get('cpv_principal', ''), l.get('cpvs_adicionales', ''))
    ]
    exptes = {str(l['expediente']) for l in lics_salud}

    lotes_salud    = [x for x in lotes    if str(x.get('expediente', '')) in exptes]
    criterios_salud = [x for x in criterios if str(x.get('expediente', '')) in exptes]
    docs_salud     = [x for x in docs     if str(x.get('expediente', '')) in exptes]

    return lics_salud, lotes_salud, criterios_salud, docs_salud


# ---------------------------------------------------------------------------
# Parseo de un mes
# ---------------------------------------------------------------------------
def parsear_mes(anio, mes):
    periodo = f'{anio}{mes}'
    all_lics, all_lotes, all_criterios, all_docs = [], [], [], []
    total_raw = 0

    for fuente, atom_name in FUENTES_SALUD:
        carpeta   = CARPETA_FUENTE[fuente]
        atom_path = os.path.join(BASE_DATOS, carpeta, 'raw', anio, mes, atom_name)
        if not os.path.exists(atom_path):
            print(f'  [AVISO] No existe: {atom_path}')
            continue
        print(f'  Parseando {fuente} {mes}/{anio}...')
        lics, lotes, crits, docs = pp.parse_atom_file(atom_path, fuente=fuente, periodo=periodo)
        total_raw += len(lics)
        lics_f, lotes_f, crits_f, docs_f = filtrar_salud(lics, lotes, crits, docs)
        print(f'    {len(lics)} licitaciones -> {len(lics_f)} salud (CPV 33*/85*) | '
              f'{len(lotes_f)} lotes | {len(crits_f)} criterios | {len(docs_f)} docs')
        all_lics.extend(lics_f)
        all_lotes.extend(lotes_f)
        all_criterios.extend(crits_f)
        all_docs.extend(docs_f)

    return all_lics, all_lotes, all_criterios, all_docs, total_raw


# ---------------------------------------------------------------------------
# Guardado CSV acumulativo
# ---------------------------------------------------------------------------
def _save_csv(df, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False, sep=';', encoding='utf-8-sig', quoting=csv.QUOTE_ALL)


def _acumular(ruta_acum, df_nuevo, expedientes_periodo):
    """
    Carga CSV acumulado existente y sustituye las filas del periodo procesado.
    Útil para re-ejecutar sin duplicar.
    """
    if os.path.exists(ruta_acum):
        df_acum = pd.read_csv(ruta_acum, sep=';', encoding='utf-8-sig', dtype={'expediente': str})
        df_acum = df_acum[~df_acum['expediente'].astype(str).isin(expedientes_periodo)]
        return pd.concat([df_acum, df_nuevo], ignore_index=True).fillna('')
    return df_nuevo.fillna('')


def guardar_csvs(lics, lotes, criterios, docs, anio, meses_procesados=None):
    """Guarda/actualiza los 4 CSVs anuales en salud/datos/csv/{anio}/"""
    df_lics  = pd.DataFrame(lics)
    df_lotes = pd.DataFrame(lotes)     if lotes     else pd.DataFrame()
    df_crits = pd.DataFrame(criterios) if criterios else pd.DataFrame()
    df_docs  = pd.DataFrame(docs)      if docs      else pd.DataFrame()

    if df_lics.empty:
        print('  Sin licitaciones salud para guardar.')
        return

    df_lics['expediente'] = df_lics['expediente'].astype(str)
    df_lics = df_lics.drop_duplicates(subset=['expediente'], keep='last')

    base_csv = os.path.join(BASE_SALUD, 'csv', anio)
    os.makedirs(base_csv, exist_ok=True)

    paths = {
        'licitaciones': os.path.join(base_csv, f'licitaciones_salud_{anio}.csv'),
        'lotes':        os.path.join(base_csv, f'lotes_salud_{anio}.csv'),
        'criterios':    os.path.join(base_csv, f'criterios_salud_{anio}.csv'),
        'documentos':   os.path.join(base_csv, f'documentos_salud_{anio}.csv'),
    }

    expedientes = set(df_lics['expediente'])

    df_total = _acumular(paths['licitaciones'], df_lics, expedientes)
    _save_csv(df_total, paths['licitaciones'])
    print(f'  licitaciones_salud_{anio}.csv : {len(df_total)} registros totales')

    if not df_lotes.empty:
        df_lotes['expediente'] = df_lotes['expediente'].astype(str)
        df_lotes_total = _acumular(paths['lotes'], df_lotes, expedientes)
        _save_csv(df_lotes_total, paths['lotes'])
        print(f'  lotes_salud_{anio}.csv       : {len(df_lotes_total)} registros totales')

    if not df_crits.empty:
        df_crits['expediente'] = df_crits['expediente'].astype(str)
        df_crits_total = _acumular(paths['criterios'], df_crits, expedientes)
        _save_csv(df_crits_total, paths['criterios'])
        print(f'  criterios_salud_{anio}.csv   : {len(df_crits_total)} registros totales')

    if not df_docs.empty:
        df_docs['expediente'] = df_docs['expediente'].astype(str)
        df_docs_total = _acumular(paths['documentos'], df_docs, expedientes)
        _save_csv(df_docs_total, paths['documentos'])
        print(f'  documentos_salud_{anio}.csv  : {len(df_docs_total)} registros totales')

    print(f'\n  Carpeta: {base_csv}')
    return df_total


# ---------------------------------------------------------------------------
# Stats rápidas
# ---------------------------------------------------------------------------
def mostrar_stats(anio='2026'):
    base_csv = os.path.join(BASE_SALUD, 'csv', anio)
    lics_path = os.path.join(base_csv, f'licitaciones_salud_{anio}.csv')
    if not os.path.exists(lics_path):
        print(f'No existe {lics_path}. Ejecuta etl_salud.py primero.')
        return

    df = pd.read_csv(lics_path, sep=';', encoding='utf-8-sig', dtype={'expediente': str})
    print(f'\n=== STATS licitaciones_salud_{anio}.csv ===')
    print(f'  Total licitaciones  : {len(df)}')

    if 'cpv_principal' in df.columns:
        print(f'\n  Por CPV (top 15):')
        for cpv, cnt in df['cpv_principal'].value_counts().head(15).items():
            print(f'    {cpv}: {cnt}')

    if 'ccaa' in df.columns:
        print(f'\n  Por CCAA (top 10):')
        for ccaa, cnt in df['ccaa'].value_counts().head(10).items():
            print(f'    {ccaa}: {cnt}')

    if 'estado' in df.columns:
        print(f'\n  Por estado:')
        for est, cnt in df['estado'].value_counts().items():
            print(f'    {est}: {cnt}')

    if 'fuente' in df.columns:
        print(f'\n  Por fuente:')
        for f, cnt in df['fuente'].value_counts().items():
            print(f'    {f}: {cnt}')

    if 'importe_licitacion_sin_iva' in df.columns:
        importes = pd.to_numeric(df['importe_licitacion_sin_iva'], errors='coerce').dropna()
        if not importes.empty:
            print(f'\n  Importe licitación s/IVA:')
            print(f'    Total  : {importes.sum():,.0f} €')
            print(f'    Mediana: {importes.median():,.0f} €')
            print(f'    Media  : {importes.mean():,.0f} €')
            print(f'    Máx    : {importes.max():,.0f} €')

    docs_path = os.path.join(base_csv, f'documentos_salud_{anio}.csv')
    if os.path.exists(docs_path):
        df_docs = pd.read_csv(docs_path, sep=';', encoding='utf-8-sig')
        print(f'\n  Documentos referenciados: {len(df_docs)}')
        if 'tipo_documento' in df_docs.columns:
            print('  Por tipo (top 10):')
            for td, cnt in df_docs['tipo_documento'].value_counts().head(10).items():
                print(f'    {td}: {cnt}')


# ---------------------------------------------------------------------------
# Detectar meses disponibles en disco
# ---------------------------------------------------------------------------
def detectar_meses_disponibles(anio):
    """Devuelve lista de meses (ej: ['01','02','03']) con atoms en disco."""
    meses = set()
    for fuente, atom_name in FUENTES_SALUD:
        carpeta = CARPETA_FUENTE[fuente]
        base = os.path.join(BASE_DATOS, carpeta, 'raw', anio)
        if os.path.exists(base):
            for m in os.listdir(base):
                atom_path = os.path.join(base, m, atom_name)
                if os.path.exists(atom_path):
                    meses.add(m)
    return sorted(meses)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description='ETL salud: licitaciones CPV 33*/85*')
    parser.add_argument('anio', nargs='?', default=None, help='Año (ej: 2026)')
    parser.add_argument('mes',  nargs='?', default=None, help='Mes (ej: 03) — omitir para todos')
    parser.add_argument('--stats', action='store_true', help='Solo mostrar estadísticas')
    args = parser.parse_args()

    anio = args.anio or '2026'

    if args.stats:
        mostrar_stats(anio)
        return

    print(f'\n{"="*60}')
    print(f'ETL SALUD (CPV 33*/85*) — Año {anio}')
    print(f'Fuentes: {", ".join(f for f,_ in FUENTES_SALUD)}')
    print(f'{"="*60}')

    if args.mes:
        meses = [args.mes.zfill(2)]
    else:
        meses = detectar_meses_disponibles(anio)
        if not meses:
            # fallback
            meses = MESES_DEFAULT_2026
        print(f'Meses detectados en disco: {meses}')

    all_lics, all_lotes, all_criterios, all_docs = [], [], [], []
    total_raw_global = 0

    for mes in meses:
        print(f'\n--- MES {mes}/{anio} ---')
        lics, lotes, crits, docs, total_raw = parsear_mes(anio, mes)
        total_raw_global += total_raw
        all_lics.extend(lics)
        all_lotes.extend(lotes)
        all_criterios.extend(crits)
        all_docs.extend(docs)

    print(f'\n{"="*60}')
    print(f'TOTAL GLOBAL ({len(meses)} meses):')
    print(f'  Raw parseadas     : {total_raw_global}')
    print(f'  Salud filtradas   : {len(all_lics)} ({len(all_lics)/max(total_raw_global,1)*100:.1f}%)')
    print(f'  Lotes             : {len(all_lotes)}')
    print(f'  Criterios         : {len(all_criterios)}')
    print(f'  Documentos        : {len(all_docs)}')
    print(f'{"="*60}')

    print(f'\nGuardando CSVs...')
    guardar_csvs(all_lics, all_lotes, all_criterios, all_docs, anio)

    print(f'\nETL SALUD completado.')
    print(f'Próximo paso: python salud/scripts/indexar_salud.py')


if __name__ == '__main__':
    main()
