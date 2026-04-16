"""
run_all.py — Orquestador: ejecuta todos los scrapers de portales
================================================================
Lanza cada scraper de portal en secuencia.
Cada portal tiene su propio handler optimizado.

Uso:
    # Todos los portales, solo pendientes
    python scrapers/run_all.py

    # Solo portales concretos
    python scrapers/run_all.py --portales euskadi madrid

    # Un expediente concreto (lo busca en todos los portales)
    python scrapers/run_all.py --expediente "2026/000008"

    # Stats de todos los portales
    python scrapers/run_all.py --stats

    # Incremental completo (visita todos los expedientes, no solo pendientes)
    python scrapers/run_all.py --todos
"""

import argparse
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from base import args_comunes, get_db, _mostrar_stats_portal

# Importar cada portal
from contratacion_estado import PortalEstado,   DOMINIO as DOM_ESTADO
from euskadi             import PortalEuskadi,  DOMINIO as DOM_EUSKADI
from madrid              import PortalMadrid,   DOMINIO as DOM_MADRID
from catalunya           import PortalCatalunya,DOMINIO as DOM_CAT
from andalucia           import PortalAndalucia,DOMINIO as DOM_AND
from navarra             import PortalNavarra,  DOMINIO as DOM_NAV

from base import ejecutar_portal

# Orden de ejecución: mayor volumen primero
PORTALES = [
    ('estado',    DOM_ESTADO,   PortalEstado),
    ('euskadi',   DOM_EUSKADI,  PortalEuskadi),
    ('madrid',    DOM_MADRID,   PortalMadrid),
    ('catalunya', DOM_CAT,      PortalCatalunya),
    ('andalucia', DOM_AND,      PortalAndalucia),
    ('navarra',   DOM_NAV,      PortalNavarra),
]

ALIAS = {
    'estado':         DOM_ESTADO,
    'contratacion':   DOM_ESTADO,
    'estatal':        DOM_ESTADO,
    'euskadi':        DOM_EUSKADI,
    'pais vasco':     DOM_EUSKADI,
    'madrid':         DOM_MADRID,
    'catalunya':      DOM_CAT,
    'cataluna':       DOM_CAT,
    'cat':            DOM_CAT,
    'andalucia':      DOM_AND,
    'junta':          DOM_AND,
    'navarra':        DOM_NAV,
}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Orquestador scrapers licitaciones salud')
    args_comunes(parser)
    parser.add_argument(
        '--portales', nargs='+', default=None,
        help='Portales a ejecutar: estado euskadi madrid catalunya andalucia navarra'
    )
    args = parser.parse_args()

    # Filtrar portales si se especificaron
    portales_a_ejecutar = PORTALES
    if args.portales:
        seleccion = {ALIAS.get(p.lower(), p.lower()) for p in args.portales}
        portales_a_ejecutar = [
            (nombre, dom, cls)
            for nombre, dom, cls in PORTALES
            if any(s in dom for s in seleccion) or nombre in seleccion
        ]

    if args.stats:
        conn = get_db()
        for nombre, dom, _ in portales_a_ejecutar:
            _mostrar_stats_portal(conn, dom)
        conn.close()
        exit(0)

    print(f'\n{"="*60}')
    print(f'Scrapers a ejecutar: {[n for n,_,_ in portales_a_ejecutar]}')
    print(f'Modo: {"incremental completo" if args.todos else "solo pendientes"}')
    print(f'{"="*60}')

    for nombre, dominio, portal_cls in portales_a_ejecutar:
        print(f'\n{"="*60}')
        print(f'PORTAL: {nombre.upper()} ({dominio})')
        print(f'{"="*60}')
        try:
            ejecutar_portal(portal_cls, dominio, args)
        except KeyboardInterrupt:
            print(f'\n[Interrumpido por usuario en {nombre}]')
            break
        except Exception as e:
            print(f'\n[ERROR fatal en {nombre}]: {e}')
            continue

    print(f'\n{"="*60}')
    print('Todos los scrapers completados.')
    print(f'{"="*60}')
