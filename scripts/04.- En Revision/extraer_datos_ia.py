"""
extraer_datos_ia.py
--------------------
Extrae campos estructurados del texto de pliegos usando Google Gemini API.

Por cada pliego (PCAP principalmente) extrae:
  - empresa_adjudicataria + nif_adjudicatario
  - importe_adjudicacion_ia
  - criterios_ia           (JSON: [{nombre, peso, tipo}])
  - solvencia_economica_ia (texto resumido)
  - solvencia_tecnica_ia   (texto resumido)
  - objeto_ia              (descripción limpia del objeto)
  - plazo_ejecucion_ia     (texto: "24 meses", "18 meses", etc.)
  - notas_ia               (cualquier dato relevante adicional)

Tabla creada: datos_ia (incremental, se puede relanzar sin duplicar)

Uso:
    # Procesar 20 pliegos de prueba
    python scripts/extraer_datos_ia.py --limite 20

    # Procesar todo
    python scripts/extraer_datos_ia.py

    # Solo un expediente concreto
    python scripts/extraer_datos_ia.py --expediente 202500000199

    # Ver estadísticas de lo ya extraído
    python scripts/extraer_datos_ia.py --stats

Requiere:
    GOOGLE_API_KEY en variables de entorno:
        setx GOOGLE_API_KEY "AIza..."
    o pasar directamente con --api-key

    Obtén la clave en: https://aistudio.google.com/apikey
"""

import argparse
import json
import os
import sqlite3
import sys
import time
from datetime import datetime

from google import genai
from google.genai import types

RUTA_DB = r'C:\proyectos\licitaciones\datos\Repositorio\licitaciones.db'

# gemini-2.0-flash: rápido, barato, muy bueno para extracción estructurada
# gemini-1.5-pro:   más potente para documentos complejos (más lento y caro)
MODELO = 'gemini-2.0-flash'

# Máx chars enviados por pliego (primeros 60k — más que Claude porque Gemini tiene más contexto)
MAX_CHARS = 60_000

PROMPT = """Eres un experto en contratación pública española.
Analiza el siguiente texto de un pliego de contratación pública y extrae estos campos en JSON:

1. empresa_adjudicataria: nombre de la empresa adjudicataria si aparece (o null)
2. nif_adjudicatario: NIF/CIF de la empresa adjudicataria (o null)
3. importe_adjudicacion_ia: importe de adjudicación en euros como número float (o null)
4. criterios_ia: lista de criterios de adjudicación:
   [{{"nombre": "...", "peso": 30, "tipo": "precio|calidad|tecnico|social|otro"}}]
   Si no hay criterios explícitos devuelve []
5. solvencia_economica_ia: requisitos de solvencia económica resumidos en 1-2 frases (o null)
6. solvencia_tecnica_ia: requisitos de solvencia técnica resumidos en 1-2 frases (o null)
7. objeto_ia: descripción limpia del objeto del contrato en 1 frase
8. plazo_ejecucion_ia: plazo de ejecución (ej: "24 meses", "1 año") (o null)
9. notas_ia: cualquier dato relevante adicional no capturado (o null)

TEXTO DEL PLIEGO:
{texto}

Responde ÚNICAMENTE con el JSON válido, sin markdown ni explicaciones.
Ejemplo:
{{
  "empresa_adjudicataria": null,
  "nif_adjudicatario": null,
  "importe_adjudicacion_ia": null,
  "criterios_ia": [{{"nombre": "Precio", "peso": 60, "tipo": "precio"}}, {{"nombre": "Calidad técnica", "peso": 40, "tipo": "calidad"}}],
  "solvencia_economica_ia": "Volumen de negocio mínimo de 500.000€ en los últimos 3 años.",
  "solvencia_tecnica_ia": "Experiencia mínima de 2 contratos similares en los últimos 5 años.",
  "objeto_ia": "Servicio de limpieza de edificios municipales.",
  "plazo_ejecucion_ia": "12 meses",
  "notas_ia": null
}}"""


def crear_tabla(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS datos_ia (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            expediente              TEXT NOT NULL,
            tipo_documento          TEXT NOT NULL,
            doc_id                  INTEGER,
            modelo                  TEXT,
            empresa_adjudicataria   TEXT,
            nif_adjudicatario       TEXT,
            importe_adjudicacion_ia REAL,
            criterios_ia            TEXT,
            solvencia_economica_ia  TEXT,
            solvencia_tecnica_ia    TEXT,
            objeto_ia               TEXT,
            plazo_ejecucion_ia      TEXT,
            notas_ia                TEXT,
            tokens_input            INTEGER DEFAULT 0,
            tokens_output           INTEGER DEFAULT 0,
            coste_usd               REAL DEFAULT 0,
            error                   TEXT,
            fecha_extraccion        TEXT,
            UNIQUE(expediente, tipo_documento)
        )
    """)
    conn.commit()


def llamar_gemini(client, texto):
    """Llama a Gemini y devuelve (datos_dict, tokens_in, tokens_out, coste_usd, error)."""
    texto_truncado = texto[:MAX_CHARS]
    try:
        resp = client.models.generate_content(
            model=MODELO,
            contents=PROMPT.format(texto=texto_truncado),
            config=types.GenerateContentConfig(
                temperature=0.1,         # baja temperatura = más determinista
                max_output_tokens=1024,
            ),
        )
        raw = resp.text.strip()

        # Limpiar posible markdown ```json ... ```
        if raw.startswith('```'):
            partes = raw.split('```')
            raw = partes[1]
            if raw.startswith('json'):
                raw = raw[4:].strip()

        datos = json.loads(raw)

        # Tokens (Gemini los reporta en usage_metadata)
        t_in  = getattr(resp.usage_metadata, 'prompt_token_count', 0) or 0
        t_out = getattr(resp.usage_metadata, 'candidates_token_count', 0) or 0

        # Coste gemini-2.0-flash: $0.075/M input, $0.30/M output (aprox)
        coste = (t_in * 0.075 + t_out * 0.30) / 1_000_000

        return datos, t_in, t_out, coste, None

    except json.JSONDecodeError as e:
        return {}, 0, 0, 0, f'JSON inválido: {str(e)[:100]} | raw: {raw[:200]}'
    except Exception as e:
        return {}, 0, 0, 0, str(e)[:300]


def mostrar_stats():
    conn = sqlite3.connect(RUTA_DB, timeout=30)
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM datos_ia")
        total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM datos_ia WHERE error IS NULL")
        ok = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM datos_ia WHERE empresa_adjudicataria IS NOT NULL")
        con_adj = cur.fetchone()[0]
        cur.execute("""SELECT COUNT(*) FROM datos_ia
                       WHERE criterios_ia NOT IN ('[]','null','') AND criterios_ia IS NOT NULL""")
        con_crit = cur.fetchone()[0]
        cur.execute("SELECT SUM(coste_usd) FROM datos_ia")
        coste = cur.fetchone()[0] or 0
        cur.execute("SELECT SUM(tokens_input+tokens_output) FROM datos_ia")
        tokens = cur.fetchone()[0] or 0

        print(f"=== STATS datos_ia ===")
        print(f"  Total procesados      : {total}")
        print(f"  Sin error             : {ok}")
        print(f"  Con adjudicataria     : {con_adj}")
        print(f"  Con criterios         : {con_crit}")
        print(f"  Coste total           : ${coste:.4f} USD")
        print(f"  Tokens totales        : {tokens:,}")

        print("\n  Muestra criterios extraídos:")
        cur.execute("""
            SELECT expediente, objeto_ia, criterios_ia
            FROM datos_ia
            WHERE criterios_ia NOT IN ('[]','null','') AND criterios_ia IS NOT NULL
            LIMIT 5
        """)
        for row in cur.fetchall():
            try:
                crits = json.loads(row[2])
                print(f"\n    [{row[0]}] {(row[1] or '')[:60]}")
                for c in crits:
                    print(f"      - {c.get('nombre','?')} ({c.get('peso','?')}%) [{c.get('tipo','?')}]")
            except Exception:
                pass

        print("\n  Muestra solvencias:")
        cur.execute("""
            SELECT expediente, solvencia_economica_ia, solvencia_tecnica_ia
            FROM datos_ia
            WHERE solvencia_economica_ia IS NOT NULL
            LIMIT 3
        """)
        for row in cur.fetchall():
            print(f"\n    [{row[0]}]")
            print(f"      Económica : {(row[1] or '')[:100]}")
            print(f"      Técnica   : {(row[2] or '')[:100]}")

    except Exception as e:
        print(f"Error: {e}")
    conn.close()


def _leer_api_key(api_key_arg=None):
    """Lee la API key: argumento > variable entorno > fichero .env"""
    if api_key_arg:
        return api_key_arg
    k = os.environ.get('GOOGLE_API_KEY', '')
    if k:
        return k
    # Buscar en .env junto al proyecto
    env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
    if os.path.exists(env_path):
        for line in open(env_path).readlines():
            line = line.strip()
            if line.startswith('GOOGLE_API_KEY='):
                return line.split('=', 1)[1].strip()
    return ''


def procesar(api_key=None, limite=None, expediente_concreto=None):
    api_key = _leer_api_key(api_key)
    if not api_key:
        print("ERROR: No se encontró GOOGLE_API_KEY.")
        print("Opciones:")
        print("  1. Fichero C:\\proyectos\\licitaciones\\.env con línea: GOOGLE_API_KEY=AIza...")
        print("  2. setx GOOGLE_API_KEY \"AIza...\" (nueva terminal)")
        print("  3. --api-key AIza...")
        sys.exit(1)

    client = genai.Client(api_key=api_key)

    conn = sqlite3.connect(RUTA_DB, timeout=30)
    crear_tabla(conn)
    cur = conn.cursor()

    query = """
        SELECT t.doc_id, t.expediente, t.tipo_documento, t.texto_raw
        FROM textos_pdf t
        WHERE t.n_chars > 500
          AND t.tipo_documento = 'pliego_administrativo'
          AND NOT EXISTS (
              SELECT 1 FROM datos_ia d
              WHERE d.expediente = t.expediente AND d.tipo_documento = t.tipo_documento
          )
    """
    params = []
    if expediente_concreto:
        query += " AND t.expediente = ?"
        params.append(expediente_concreto)
    if limite:
        query += f" LIMIT {limite}"

    cur.execute(query, params)
    docs = cur.fetchall()

    total = len(docs)
    print(f"Pliegos a procesar con Gemini ({MODELO}): {total}")
    if total == 0:
        print("Nada pendiente. Usa --stats para ver resultados.")
        conn.close()
        return

    ok = errores = 0
    coste_total = 0.0
    ahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    for i, (doc_id, expediente, tipo_doc, texto) in enumerate(docs, 1):
        if not texto:
            continue

        datos, t_in, t_out, coste, error = llamar_gemini(client, texto)
        coste_total += coste

        if error:
            errores += 1
            cur.execute("""
                INSERT OR REPLACE INTO datos_ia
                (expediente, tipo_documento, doc_id, modelo, error, fecha_extraccion)
                VALUES (?,?,?,?,?,?)
            """, (expediente, tipo_doc, doc_id, MODELO, error, ahora))
        else:
            ok += 1
            cur.execute("""
                INSERT OR REPLACE INTO datos_ia
                (expediente, tipo_documento, doc_id, modelo,
                 empresa_adjudicataria, nif_adjudicatario, importe_adjudicacion_ia,
                 criterios_ia, solvencia_economica_ia, solvencia_tecnica_ia,
                 objeto_ia, plazo_ejecucion_ia, notas_ia,
                 tokens_input, tokens_output, coste_usd, fecha_extraccion)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                expediente, tipo_doc, doc_id, MODELO,
                datos.get('empresa_adjudicataria'),
                datos.get('nif_adjudicatario'),
                datos.get('importe_adjudicacion_ia'),
                json.dumps(datos.get('criterios_ia', []), ensure_ascii=False),
                datos.get('solvencia_economica_ia'),
                datos.get('solvencia_tecnica_ia'),
                datos.get('objeto_ia'),
                datos.get('plazo_ejecucion_ia'),
                datos.get('notas_ia'),
                t_in, t_out, coste,
                ahora,
            ))

        if i % 20 == 0:
            conn.commit()
            pct = i / total * 100
            print(f"  [{i}/{total} {pct:.0f}%] OK:{ok} err:{errores} coste:${coste_total:.4f}")

        # Pausa mínima — Gemini Pro aguanta bien el ritmo
        time.sleep(0.1)

    conn.commit()
    conn.close()

    print(f"\n{'='*40}")
    print(f"Extracción IA completada")
    print(f"  Procesados : {total}")
    print(f"  OK         : {ok}")
    print(f"  Errores    : {errores}")
    print(f"  Coste total: ${coste_total:.4f} USD")
    print(f"{'='*40}")
    print(f"\nUsa --stats para ver los datos extraídos.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Extracción IA de pliegos con Gemini')
    parser.add_argument('--limite', type=int, default=None,
                        help='Procesar solo N pliegos (útil para pruebas)')
    parser.add_argument('--expediente', type=str, default=None,
                        help='Procesar solo este expediente concreto')
    parser.add_argument('--api-key', type=str, default=None,
                        help='Google API key (alternativa a variable de entorno)')
    parser.add_argument('--stats', action='store_true',
                        help='Mostrar estadísticas de extracciones ya realizadas')
    parser.add_argument('--modelo', type=str, default=MODELO,
                        help=f'Modelo Gemini a usar (default: {MODELO})')
    args = parser.parse_args()

    if args.modelo != MODELO:
        MODELO = args.modelo

    if args.stats:
        mostrar_stats()
    else:
        procesar(api_key=args.api_key, limite=args.limite,
                 expediente_concreto=args.expediente)
