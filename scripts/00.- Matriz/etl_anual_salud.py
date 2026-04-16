"""
etl_anual_salud.py — Procesa todos los meses de un año completo (sector salud)
===============================================================================
Llama a etl_salud.py mes a mes, omitiendo los meses futuros.

Uso:
    python "scripts/00.- Matriz/etl_anual_salud.py" 2025
    python "scripts/00.- Matriz/etl_anual_salud.py" 2026
"""

import subprocess, sys, os
from datetime import datetime
from pathlib import Path


def ejecutar_mes(anio, mes):
    mes_str = str(mes).zfill(2)
    print(f'\n{"="*50}')
    print(f'  PROCESANDO {mes_str}/{anio}')
    print(f'{"="*50}')
    etl_path = str(Path(__file__).resolve().parent / 'etl_salud.py')
    resultado = subprocess.run(
        [sys.executable, etl_path, str(anio), mes_str],
        cwd=str(Path(__file__).resolve().parent.parent.parent),  # raíz del proyecto salud
        capture_output=False
    )
    return resultado.returncode == 0


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Uso: python "scripts/00.- Matriz/etl_anual_salud.py" AAAA')
        print('Ejemplo: python "scripts/00.- Matriz/etl_anual_salud.py" 2025')
        sys.exit(1)

    anio      = int(sys.argv[1])
    anio_act  = datetime.now().year
    mes_act   = datetime.now().month
    resultados = []

    for mes in range(1, 13):
        if anio == anio_act and mes > mes_act:
            print(f'  Mes {str(mes).zfill(2)}/{anio}: omitido (mes futuro)')
            continue
        ok = ejecutar_mes(anio, mes)
        resultados.append((mes, ok))

    print(f'\n{"="*50}')
    print(f'  RESUMEN ETL ANUAL SALUD {anio}')
    print(f'{"="*50}')
    for mes, ok in resultados:
        estado = 'OK   ' if ok else 'ERROR'
        print(f'  {estado} | {str(mes).zfill(2)}/{anio}')
