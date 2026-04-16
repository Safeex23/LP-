"""
reconciliar_pdfs.py
-------------------
Recorre datos/Repositorio/pdfs/{expediente}/ y actualiza ruta_local
en la tabla documentos para pcap.pdf (pliego_administrativo) y
ppt.pdf (pliego_tecnico).

Uso:
    python scripts/reconciliar_pdfs.py
"""

import os
import sqlite3

RUTA_DB  = r'C:\proyectos\licitaciones\datos\Repositorio\licitaciones.db'
RUTA_PDF = r'C:\proyectos\licitaciones\datos\Repositorio\pdfs'

FILENAME_TO_TIPO = {
    'pcap.pdf': 'pliego_administrativo',
    'ppt.pdf':  'pliego_tecnico',
}


def reconciliar():
    conn = sqlite3.connect(RUTA_DB, timeout=30)
    cur  = conn.cursor()

    actualizados = 0
    no_encontrados = 0

    expedientes = [d for d in os.listdir(RUTA_PDF)
                   if os.path.isdir(os.path.join(RUTA_PDF, d))]

    print(f"Expedientes en disco: {len(expedientes)}")

    for exp in expedientes:
        exp_dir = os.path.join(RUTA_PDF, exp)
        for filename, tipo in FILENAME_TO_TIPO.items():
            ruta_abs = os.path.join(exp_dir, filename)
            if not os.path.exists(ruta_abs):
                continue

            # Buscar fila en BD: mismo expediente y tipo_documento
            cur.execute("""
                SELECT id FROM documentos
                WHERE expediente = ? AND tipo_documento = ?
                AND (ruta_local IS NULL OR ruta_local = '')
                LIMIT 1
            """, (exp, tipo))
            row = cur.fetchone()

            if row:
                cur.execute("""
                    UPDATE documentos
                    SET ruta_local = ?, descargado = 1
                    WHERE id = ?
                """, (ruta_abs, row[0]))
                actualizados += 1
            else:
                # Ya tiene ruta o no hay fila — verificar si ya estaba actualizado
                cur.execute("""
                    SELECT id FROM documentos
                    WHERE expediente = ? AND tipo_documento = ?
                    LIMIT 1
                """, (exp, tipo))
                if not cur.fetchone():
                    no_encontrados += 1

        if actualizados % 500 == 0 and actualizados > 0:
            conn.commit()
            print(f"  {actualizados} actualizados...")

    conn.commit()
    conn.close()

    print(f"\nReconciliación completa:")
    print(f"  Rutas actualizadas : {actualizados}")
    print(f"  Sin fila en BD     : {no_encontrados}")

    # Stats finales
    conn = sqlite3.connect(RUTA_DB, timeout=30)
    cur  = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM documentos WHERE descargado = 1")
    print(f"  Docs con descargado=1 en BD: {cur.fetchone()[0]}")
    cur.execute("""
        SELECT tipo_documento, COUNT(*) FROM documentos
        WHERE descargado = 1
        GROUP BY tipo_documento ORDER BY COUNT(*) DESC
    """)
    print("\nPor tipo:")
    for row in cur.fetchall():
        print(f"  {row[0]}: {row[1]}")
    conn.close()


if __name__ == '__main__':
    reconciliar()
