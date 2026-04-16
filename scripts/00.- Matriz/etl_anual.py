# etl_anual.py - Procesa todos los meses de un ano completo

import subprocess, sys, os
from datetime import datetime
from pathlib import Path

def ejecutar_mes(anio, mes):
    mes_str = str(mes).zfill(2)
    print(f'PROCESANDO {mes_str}/{anio}')
    etl_path = str(Path(__file__).resolve().parent / 'etl.py')
    resultado = subprocess.run(
        [sys.executable, etl_path, str(anio), mes_str],
        cwd=str(Path(__file__).resolve().parent.parent),
        capture_output=False
    )
    return resultado.returncode == 0

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Uso: python scripts/etl_anual.py AAAA')
        sys.exit(1)
    anio = int(sys.argv[1])
    anio_actual = datetime.now().year
    mes_actual  = datetime.now().month
    resultados = []
    for mes in range(1, 13):
        if anio == anio_actual and mes > mes_actual:
            print(f'  Mes {str(mes).zfill(2)}/{anio}: omitido (mes futuro)')
            continue
        ok = ejecutar_mes(anio, mes)
        resultados.append((mes, ok))
    print('=== RESUMEN ETL ANUAL ===')
    for mes, ok in resultados:
        print(f'  {"OK" if ok else "ERROR"} | {str(mes).zfill(2)}/{anio}')
