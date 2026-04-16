# descarga_atom.py - Descarga automática de los 3 ficheros .atom de PLACSP
# Descarga siempre el mes en curso. Ejecutar 1 vez al mes o programar con Task Scheduler.

import requests
import zipfile
import os
from datetime import datetime, timedelta
import calendar

# ── CONFIGURACIÓN ─────────────────────────────────────────────────────────────
hoy = datetime.now()

if hoy.day == 1:
    primer_dia = hoy.replace(day=1)
    mes_ref    = primer_dia - timedelta(days=1)
else:
    mes_ref = hoy

ANIO = mes_ref.strftime('%Y')
MES  = mes_ref.strftime('%m')
AAMM = f"{ANIO}{MES}"

FUENTES = [
    {
        'nombre': 'Sector Publico',
        'url':    f"https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_643/licitacionesPerfilesContratanteCompleto3_{AAMM}.zip",
        'zip':    f"C:\\proyectos\\licitaciones\\datos\\Sector Publico\\raw\\{ANIO}\\{MES}\\sector_publico_{AAMM}.zip",
        'destino':f"C:\\proyectos\\licitaciones\\datos\\Sector Publico\\raw\\{ANIO}\\{MES}\\",
        'atom':   'licitacionesPerfilesContratanteCompleto3.atom',
    },
    {
        'nombre': 'Agregacion',
        'url':    f"https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_1044/PlataformasAgregadasSinMenores_{AAMM}.zip",
        'zip':    f"C:\\proyectos\\licitaciones\\datos\\Agregacion\\raw\\{ANIO}\\{MES}\\agregadas_{AAMM}.zip",
        'destino':f"C:\\proyectos\\licitaciones\\datos\\Agregacion\\raw\\{ANIO}\\{MES}\\",
        'atom':   'PlataformasAgregadasSinMenores.atom',
    },
    {
        'nombre': 'Contratos Menores',
        'url':    f"https://contrataciondelsectorpublico.gob.es/sindicacion/sindicacion_1143/contratosMenoresPerfilesContratantes_{AAMM}.zip",
        'zip':    f"C:\\proyectos\\licitaciones\\datos\\Menores\\raw\\{ANIO}\\{MES}\\menores_{AAMM}.zip",
        'destino':f"C:\\proyectos\\licitaciones\\datos\\Menores\\raw\\{ANIO}\\{MES}\\",
        'atom':   'contratosMenoresPerfilesContratantes.atom',
    },
]

# ── FUNCIONES ─────────────────────────────────────────────────────────────────
def descargar_zip(nombre, url, ruta_zip):
    print(f"\n[{nombre}] Descargando ZIP...")
    print(f"  URL: {url}")
    os.makedirs(os.path.dirname(ruta_zip), exist_ok=True)
    try:
        r = requests.get(url, timeout=120, stream=True)
        r.raise_for_status()
        with open(ruta_zip, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        tam = os.path.getsize(ruta_zip) / (1024 * 1024)
        print(f"  ZIP descargado: {tam:.1f} MB")
        return True
    except Exception as e:
        print(f"  ERROR descargando {nombre}: {e}")
        return False

def descomprimir_zip(nombre, ruta_zip, destino, atom_esperado):
    print(f"[{nombre}] Descomprimiendo...")
    try:
        with zipfile.ZipFile(ruta_zip, 'r') as z:
            archivos = z.namelist()
            print(f"  Archivos en ZIP: {archivos}")
            z.extractall(destino)
        atom_path = os.path.join(destino, atom_esperado)
        if os.path.exists(atom_path):
            tam = os.path.getsize(atom_path) / (1024 * 1024)
            print(f"  .atom extraido: {atom_esperado} ({tam:.1f} MB)")
            os.remove(ruta_zip)
            print(f"  ZIP eliminado.")
            return True
        else:
            print(f"  AVISO: No se encontro {atom_esperado} en el ZIP.")
            print(f"  Archivos disponibles: {archivos}")
            return False
    except Exception as e:
        print(f"  ERROR descomprimiendo {nombre}: {e}")
        return False

# ── EJECUCIÓN ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"=== DESCARGA ATOM PLACSP ===")
    print(f"Periodo: {MES}/{ANIO}\n")

    resultados = []
    for f in FUENTES:
        ok_zip  = descargar_zip(f['nombre'], f['url'], f['zip'])
        ok_atom = False
        if ok_zip:
            ok_atom = descomprimir_zip(f['nombre'], f['zip'], f['destino'], f['atom'])
        resultados.append((f['nombre'], ok_zip, ok_atom))

    print("\n=== RESUMEN ===")
    for nombre, ok_zip, ok_atom in resultados:
        estado = "OK" if ok_zip and ok_atom else "ERROR"
        print(f"  {estado} | {nombre}")

    todos_ok = all(ok_zip and ok_atom for _, ok_zip, ok_atom in resultados)
    if todos_ok:
        print("\nTodos los ficheros descargados y listos. Puedes ejecutar etl.py.")
    else:
        print("\nHay errores. Revisa los mensajes anteriores antes de continuar.")