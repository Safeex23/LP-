# etl.py - Pipeline completo: descarga + parseo + unificacion (arquitectura 4 CSVs)
# Uso: python scripts/etl.py [AAAA MM]
# Sin argumentos: periodo automatico (mes actual o mes anterior el dia 1)

import sys
import os
import csv
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

# Add 01.- Produccion to path so we can import parse_placsp, indexar, descarga_atom
_PROD = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '01.- Produccion')
sys.path.insert(0, os.path.normpath(_PROD))
import descarga_atom
import parse_placsp as pp
import indexar

BASE     = r'C:\proyectos\licitaciones\datos'
BASE_OD  = r'C:\Users\sfenoll\OneDrive - INTEGRA\Documentos\03.- Licitaciones\Maestro\csv'

FUENTES = [
    ('Sector Publico',    'licitacionesPerfilesContratanteCompleto3.atom'),
    ('Agregacion',        'PlataformasAgregadasSinMenores.atom'),
    ('Contratos Menores', 'contratosMenoresPerfilesContratantes.atom'),
]
CARPETA_FUENTE = {
    'Sector Publico':    'Sector Publico',
    'Agregacion':        'Agregacion',
    'Contratos Menores': 'Menores',
}
URL_FUENTE = {
    'Sector Publico':    'https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_643/licitacionesPerfilesContratanteCompleto3_{aamm}.zip',
    'Agregacion':        'https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_1044/PlataformasAgregadasSinMenores_{aamm}.zip',
    'Contratos Menores': 'https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_1143/contratosMenoresPerfilesContratantes_{aamm}.zip',
}


def calcular_periodo(anio_arg=None, mes_arg=None):
    if anio_arg and mes_arg:
        return str(anio_arg), str(mes_arg).zfill(2)
    hoy = datetime.now()
    if hoy.day == 1:
        mes_ref = hoy.replace(day=1) - timedelta(days=1)
    else:
        mes_ref = hoy
    return mes_ref.strftime('%Y'), mes_ref.strftime('%m')


def paso_descarga(anio, mes):
    print(f"\n{'='*50}")
    print(f"PASO 1: DESCARGA ({mes}/{anio})")
    print(f"{'='*50}")
    aamm = f'{anio}{mes}'
    fuentes_config = []
    for fuente, atom_name in FUENTES:
        carpeta  = CARPETA_FUENTE[fuente]
        zip_name = atom_name.replace('.atom', f'_{aamm}.zip')
        fuentes_config.append({
            'nombre':  fuente,
            'url':     URL_FUENTE[fuente].format(aamm=aamm),
            'zip':     os.path.join(BASE, carpeta, 'raw', anio, mes, zip_name),
            'destino': os.path.join(BASE, carpeta, 'raw', anio, mes) + '\\',
            'atom':    atom_name,
        })
    resultados = []
    for f in fuentes_config:
        ok_zip  = descarga_atom.descargar_zip(f['nombre'], f['url'], f['zip'])
        ok_atom = descarga_atom.descomprimir_zip(f['nombre'], f['zip'], f['destino'], f['atom']) if ok_zip else False
        resultados.append((f['nombre'], ok_zip and ok_atom))
    errores = [n for n, ok in resultados if not ok]
    if errores:
        print(f"\nERROR en descarga: {errores}")
        return False
    print("\nDescarga completada OK.")
    return True


def paso_parseo(anio, mes):
    print(f"\n{'='*50}")
    print(f"PASO 2: PARSEO ({mes}/{anio})")
    print(f"{'='*50}")
    periodo = f'{anio}{mes}'
    all_lics, all_lotes, all_criterios, all_docs = [], [], [], []
    for fuente, atom_name in FUENTES:
        carpeta   = CARPETA_FUENTE[fuente]
        atom_path = os.path.join(BASE, carpeta, 'raw', anio, mes, atom_name)
        if not os.path.exists(atom_path):
            print(f'  AVISO: no existe {atom_path}')
            continue
        print(f'  Parseando {fuente}...')
        lics, lotes, crits, docs = pp.parse_atom_file(atom_path, fuente=fuente, periodo=periodo)
        print(f'    {len(lics)} licitaciones, {len(lotes)} lotes, {len(crits)} criterios, {len(docs)} documentos')
        all_lics.extend(lics)
        all_lotes.extend(lotes)
        all_criterios.extend(crits)
        all_docs.extend(docs)
    print(f'  TOTAL: {len(all_lics)} licitaciones, {len(all_lotes)} lotes, {len(all_criterios)} criterios, {len(all_docs)} documentos')
    return all_lics, all_lotes, all_criterios, all_docs


def _save_csv(df, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False, sep=';', encoding='utf-8-sig', quoting=csv.QUOTE_ALL)


def _acumular(ruta_acum, df_nuevo, expedientes_mes):
    """Carga CSV acumulado anual, descarta filas del periodo actual y concatena las nuevas."""
    if os.path.exists(ruta_acum):
        df_acum = pd.read_csv(ruta_acum, sep=';', encoding='utf-8-sig', dtype={'expediente': str}, low_memory=False)
        df_acum = df_acum[~df_acum['expediente'].astype(str).isin(expedientes_mes)]
        return pd.concat([df_acum, df_nuevo], ignore_index=True).fillna('')
    return df_nuevo.fillna('')


def paso_unificar(lics, lotes, criterios, docs, anio, mes):
    print(f"\n{'='*50}")
    print(f"PASO 3: UNIFICACION ({mes}/{anio})")
    print(f"{'='*50}")

    df_lics  = pd.DataFrame(lics)
    df_lotes = pd.DataFrame(lotes)    if lotes else pd.DataFrame()
    df_crits = pd.DataFrame(criterios) if criterios else pd.DataFrame()
    df_docs  = pd.DataFrame(docs)      if docs else pd.DataFrame()

    df_lics['expediente'] = df_lics['expediente'].astype(str)
    df_lics = df_lics.drop_duplicates(subset=['expediente'], keep='last')
    print(f"  Licitaciones del mes (dedup): {len(df_lics)}")

    base_csv = os.path.join(BASE_OD, anio)
    os.makedirs(base_csv, exist_ok=True)
    paths = {
        'licitaciones': os.path.join(base_csv, f'licitaciones_{anio}.csv'),
        'lotes':        os.path.join(base_csv, f'lotes_{anio}.csv'),
        'criterios':    os.path.join(base_csv, f'criterios_{anio}.csv'),
        'documentos':   os.path.join(base_csv, f'documentos_{anio}.csv'),
    }

    expedientes_mes = set(df_lics['expediente'].astype(str))

    # Acumular licitaciones
    df_total = _acumular(paths['licitaciones'], df_lics, expedientes_mes)
    _save_csv(df_total, paths['licitaciones'])
    print(f"  licitaciones_{anio}.csv: {len(df_total)} registros")

    # Acumular lotes
    if not df_lotes.empty:
        df_lotes['expediente'] = df_lotes['expediente'].astype(str)
        df_lotes_total = _acumular(paths['lotes'], df_lotes, expedientes_mes)
        _save_csv(df_lotes_total, paths['lotes'])
        print(f"  lotes_{anio}.csv: {len(df_lotes_total)} registros")

    # Acumular criterios
    if not df_crits.empty:
        df_crits['expediente'] = df_crits['expediente'].astype(str)
        df_crits_total = _acumular(paths['criterios'], df_crits, expedientes_mes)
        _save_csv(df_crits_total, paths['criterios'])
        print(f"  criterios_{anio}.csv: {len(df_crits_total)} registros")

    # Acumular documentos
    if not df_docs.empty:
        df_docs['expediente'] = df_docs['expediente'].astype(str)
        df_docs_total = _acumular(paths['documentos'], df_docs, expedientes_mes)
        _save_csv(df_docs_total, paths['documentos'])
        print(f"  documentos_{anio}.csv: {len(df_docs_total)} registros")

    print(f"\n  Carpeta OneDrive: {base_csv}")
    return df_total


if __name__ == '__main__':
    args = sys.argv[1:]
    anio, mes = calcular_periodo(args[0], args[1]) if len(args) == 2 else calcular_periodo()
    print(f"\n=== ETL LICITACIONES | Periodo: {mes}/{anio} ===")

    ok = paso_descarga(anio, mes)
    if not ok:
        print("\nPipeline detenido por error en descarga.")
        sys.exit(1)

    lics, lotes, criterios, docs = paso_parseo(anio, mes)
    paso_unificar(lics, lotes, criterios, docs, anio, mes)

    print(f"\n{'='*50}")
    print("PASO 4: INDEXACION EN REPOSITORIO")
    print(f"{'='*50}")
    os.makedirs(os.path.dirname(indexar.RUTA_DB), exist_ok=True)
    conn = sqlite3.connect(indexar.RUTA_DB, timeout=30)
    indexar.crear_db(conn)
    indexar.indexar_todo(conn, anio)
    total = conn.execute('SELECT COUNT(*) FROM licitaciones').fetchone()[0]
    total_docs = conn.execute('SELECT COUNT(*) FROM documentos').fetchone()[0]
    conn.close()

    print(f"\n=== PIPELINE COMPLETADO ===")
    print(f"  Licitaciones en BD: {total}")
    print(f"  Documentos en BD:   {total_docs}")
