# etl.py - Pipeline completo: descarga + parseo + unificación
# Uso: python etl.py AAAA MM
# Ejemplo: python etl.py 2026 03
# Sin argumentos: usa la lógica de fecha automática (mes actual o anterior si día 1)

import sys
import os
import pandas as pd
from datetime import datetime, timedelta

from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext

# ── IMPORTAR SCRIPTS EXISTENTES ───────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import descarga_atom
import parseo_sector_publico    as parseo_sp
import parseo_agregacion        as parseo_agr
import parseo_contratos_menores as parseo_men

# ── RUTAS BASE ────────────────────────────────────────────────────────────────
BASE = r"C:\proyectos\licitaciones\datos"
SP_URL      = "https://bauhost.sharepoint.com/sites/Licitaciones919"
SP_USUARIO  = "sfenoll@integrateconoloia.es"       # cambia por tu email
SP_PASSWORD = "1nt3gra.2026!"              # cambia por tu contraseña
SP_CARPETA  = "Documentos compartidos/Licitaciones/Archivos"

# ── LÓGICA DE FECHA ───────────────────────────────────────────────────────────
def calcular_periodo(anio_arg=None, mes_arg=None):
    if anio_arg and mes_arg:
        return str(anio_arg), str(mes_arg).zfill(2)
    hoy = datetime.now()
    if hoy.day == 1:
        mes_ref = hoy.replace(day=1) - timedelta(days=1)
    else:
        mes_ref = hoy
    return mes_ref.strftime('%Y'), mes_ref.strftime('%m')

# ── RUTAS DINÁMICAS POR MES ───────────────────────────────────────────────────
def rutas(anio, mes):
    return {
        'sp': {
            'atom': os.path.join(BASE, "Sector Publico", "raw", anio, mes, "licitacionesPerfilesContratanteCompleto3.atom"),
            'csv':  os.path.join(BASE, "Sector Publico", "csv", anio, mes, "licitaciones_final.csv"),
        },
        'agr': {
            'atom': os.path.join(BASE, "Agregacion", "raw", anio, mes, "PlataformasAgregadasSinMenores.atom"),
            'csv':  os.path.join(BASE, "Agregacion", "csv", anio, mes, "agregadas_final.csv"),
        },
        'men': {
            'atom': os.path.join(BASE, "Menores", "raw", anio, mes, "contratosMenoresPerfilesContratantes.atom"),
            'csv':  os.path.join(BASE, "Menores", "csv", anio, mes, "contratos_menores_final.csv"),
        },
    }

# ── PASO 1: DESCARGA ──────────────────────────────────────────────────────────
def paso_descarga(anio, mes):
    print(f"\n{'='*50}")
    print(f"PASO 1: DESCARGA ({mes}/{anio})")
    print(f"{'='*50}")
    descarga_atom.ANIO = anio
    descarga_atom.MES  = mes
    descarga_atom.AAMM = f"{anio}{mes}"

    # Reconstruir FUENTES con las fechas correctas
    aamm = f"{anio}{mes}"
    fuentes = [
        {
            'nombre': 'Sector Publico',
            'url':    f"https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_643/licitacionesPerfilesContratanteCompleto3_{aamm}.zip",
            'zip':    os.path.join(BASE, "Sector Publico", "raw", anio, mes, f"sector_publico_{aamm}.zip"),
            'destino':os.path.join(BASE, "Sector Publico", "raw", anio, mes) + "\\",
            'atom':   'licitacionesPerfilesContratanteCompleto3.atom',
        },
        {
            'nombre': 'Agregacion',
            'url':    f"https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_1044/PlataformasAgregadasSinMenores_{aamm}.zip",
            'zip':    os.path.join(BASE, "Agregacion", "raw", anio, mes, f"agregadas_{aamm}.zip"),
            'destino':os.path.join(BASE, "Agregacion", "raw", anio, mes) + "\\",
            'atom':   'PlataformasAgregadasSinMenores.atom',
        },
        {
            'nombre': 'Contratos Menores',
            'url':    f"https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_1143/contratosMenoresPerfilesContratantes_{aamm}.zip",
            'zip':    os.path.join(BASE, "Menores", "raw", anio, mes, f"menores_{aamm}.zip"),
            'destino':os.path.join(BASE, "Menores", "raw", anio, mes) + "\\",
            'atom':   'contratosMenoresPerfilesContratantes.atom',
        },
    ]

    resultados = []
    for f in fuentes:
        ok_zip  = descarga_atom.descargar_zip(f['nombre'], f['url'], f['zip'])
        ok_atom = False
        if ok_zip:
            ok_atom = descarga_atom.descomprimir_zip(f['nombre'], f['zip'], f['destino'], f['atom'])
        resultados.append((f['nombre'], ok_zip and ok_atom))

    errores = [n for n, ok in resultados if not ok]
    if errores:
        print(f"\nERROR en descarga: {errores}")
        return False
    print("\nDescarga completada OK.")
    return True

# ── PASO 2: PARSEO ────────────────────────────────────────────────────────────
def paso_parseo(anio, mes):
    print(f"\n{'='*50}")
    print(f"PASO 2: PARSEO ({mes}/{anio})")
    print(f"{'='*50}")
    r = rutas(anio, mes)

    # Sector Publico
    df_sp = parseo_sp.parsear_atom(r['sp']['atom'])
    df_sp['expediente'] = df_sp['expediente'].astype(str)
    os.makedirs(os.path.dirname(r['sp']['csv']), exist_ok=True)
    df_sp.to_csv(r['sp']['csv'], index=False, sep=';', encoding='utf-8-sig')
    print(f"  CSV Sector Publico: {r['sp']['csv']}")

    # Agregadas
    df_agr = parseo_agr.parsear_atom(r['agr']['atom'])
    df_agr['expediente'] = df_agr['expediente'].astype(str)
    os.makedirs(os.path.dirname(r['agr']['csv']), exist_ok=True)
    df_agr.to_csv(r['agr']['csv'], index=False, sep=';', encoding='utf-8-sig')
    print(f"  CSV Agregadas: {r['agr']['csv']}")

    # Menores
    df_men = parseo_men.parsear_atom(r['men']['atom'])
    df_men['expediente'] = df_men['expediente'].astype(str)
    os.makedirs(os.path.dirname(r['men']['csv']), exist_ok=True)
    df_men.to_csv(r['men']['csv'], index=False, sep=';', encoding='utf-8-sig')
    print(f"  CSV Menores: {r['men']['csv']}")

    return df_sp, df_agr, df_men

# ── PASO 3: UNIFICACIÓN Y ACUMULADO ──────────────────────────────────────────
def paso_unificar(df_sp, df_agr, df_men, anio, mes):
    print(f"\n{'='*50}")
    print(f"PASO 3: UNIFICACIÓN ({mes}/{anio})")
    print(f"{'='*50}")

    df_mes = pd.concat([df_sp, df_agr, df_men], ignore_index=True)
    df_mes['expediente'] = df_mes['expediente'].astype(str)
    df_mes['fecha_carga'] = pd.Timestamp.now().strftime('%d/%m/%Y %H:%M')
    df_mes['periodo'] = f"{anio}{mes}"
    print(f"  Registros del mes antes de deduplicar: {len(df_mes)}")
    df_mes = df_mes.drop_duplicates(subset=['expediente'], keep='last')
    print(f"  Registros del mes después de deduplicar: {len(df_mes)}")

    # Cargar acumulado existente si existe
    RUTA_MAESTRO_ACUMULADO = os.path.join(BASE, "Maestro", "csv", f"licitaciones_maestro_{anio}.csv")
    os.makedirs(os.path.dirname(RUTA_MAESTRO_ACUMULADO), exist_ok=True)
    if os.path.exists(RUTA_MAESTRO_ACUMULADO):
        df_acum = pd.read_csv(RUTA_MAESTRO_ACUMULADO, sep=';', encoding='utf-8-sig', dtype={'expediente': str})
        print(f"  Acumulado existente: {len(df_acum)} registros")
        # Eliminar registros del mismo periodo para reemplazarlos
        df_acum = df_acum[df_acum['periodo'] != f"{anio}{mes}"]
        df_total = pd.concat([df_acum, df_mes], ignore_index=True)
    else:
        print(f"  No existe acumulado previo. Creando nuevo.")
        df_total = df_mes

    # ── MAESTRO COMPLETO (todas las versiones, para PDFs y trazabilidad) ──────
    RUTA_MAESTRO_COMPLETO = os.path.join(BASE, "Maestro", "csv", anio, f"licitaciones_maestro_completo_{anio}.csv")
    df_total.to_csv(RUTA_MAESTRO_COMPLETO, index=False, sep=';', encoding='utf-8-sig')
    print(f"\n  Maestro completo (PDFs/trazabilidad): {len(df_total)} registros")
    print(f"  Ruta: {RUTA_MAESTRO_COMPLETO}")

    # ── MAESTRO REDUCIDO (última versión por expediente, para Power BI) ────────
    RUTA_MAESTRO_BI = os.path.join(BASE, "Maestro", "csv", anio, f"licitaciones_maestro_powerbi_{anio}.csv")
    df_bi = df_total.copy()
    df_bi = df_bi.sort_values('fecha_carga', ascending=True)
    df_bi = df_bi.drop_duplicates(subset=['expediente'], keep='last')
    df_bi.to_csv(RUTA_MAESTRO_BI, index=False, sep=';', encoding='utf-8-sig')
    print(f"\n  Maestro Power BI (última versión): {len(df_bi)} registros")
    print(f"  Ruta: {RUTA_MAESTRO_BI}")

    ## Esto genera dos ficheros en `Maestro\csv\` por cada año:
    ## licitaciones_maestro_completo_2026.csv   → PDFs y trazabilidad
    ## licitaciones_maestro_powerbi_2026.csv    → Power BI

    print(f"\n  Total acumulado: {len(df_total)} registros")
    print(f"  CSV maestro: {RUTA_MAESTRO_ACUMULADO}")
    print(f"\n  Distribución por fuente:")
    print(df_total['fuente'].value_counts().to_string())
    print(f"\n  Distribución por periodo:")
    df_total['periodo'] = df_total['periodo'].astype(str)
    print(df_total['periodo'].value_counts().sort_index().to_string())

# ── PASO 4: SUBIDA A SHAREPOINT ───────────────────────────────────────────────
def paso_sharepoint(anio):
    print(f"\n{'='*50}")
    print(f"PASO 4: SUBIDA A SHAREPOINT ({anio})")
    print(f"{'='*50}")

    ruta_completo = os.path.join(BASE, "Maestro", "csv", anio, f"licitaciones_maestro_completo_{anio}.csv")
    ruta_bi       = os.path.join(BASE, "Maestro", "csv", anio, f"licitaciones_maestro_powerbi_{anio}.csv")

    try:
        ctx = ClientContext(SP_URL).with_credentials(UserCredential(SP_USUARIO, SP_PASSWORD))
        carpeta = ctx.web.get_folder_by_server_relative_url(SP_CARPETA)

        for ruta_local in [ruta_completo, ruta_bi]:
            nombre_fichero = os.path.basename(ruta_local)
            with open(ruta_local, 'rb') as f:
                carpeta.upload_file(nombre_fichero, f.read()).execute_query()
            print(f"  Subido: {nombre_fichero}")

        print(f"\n  Subida completada OK.")
        return True

    except Exception as e:
        print(f"  ERROR subiendo a SharePoint: {e}")
        print(f"  Los CSV siguen disponibles en local.")
        return False
    
# ── MAIN ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) == 2:
        anio, mes = calcular_periodo(args[0], args[1])
    else:
        anio, mes = calcular_periodo()

    print(f"\n=== ETL LICITACIONES | Periodo: {mes}/{anio} ===")

    ok = paso_descarga(anio, mes)
    if not ok:
        print("\nPipeline detenido por error en descarga.")
        sys.exit(1)

    df_sp, df_agr, df_men = paso_parseo(anio, mes)
    paso_unificar(df_sp, df_agr, df_men, anio, mes)
    paso_sharepoint(anio)

    print(f"\n=== PIPELINE COMPLETADO ===")