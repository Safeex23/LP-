# app.py - Repositorio web de licitaciones (arquitectura 4 CSVs / parse_placsp)
# Uso: python scripts/app.py
# Acceso: http://localhost:5000

import csv
import io
import os
import sqlite3

from flask import Flask, abort, make_response, render_template_string, request, send_file

app = Flask(__name__)

RUTA_DB   = r'C:\proyectos\licitaciones\datos\Repositorio\licitaciones.db'
RUTA_PDFS = r'C:\proyectos\licitaciones\datos\Repositorio\pdfs'

MAP_TIPO_CONTRATO = {
    '1': 'Obras', '2': 'Concesion de obras', '3': 'Gestion de servicios publicos',
    '4': 'Suministros', '5': 'Servicios', '6': 'Concesion de servicios',
    '7': 'Administrativo especial', '8': 'Privado', '21': 'Patrimonio',
    '22': 'Arrendamiento', '31': 'Colaboracion publico privada',
    '32': 'Otros', '40': 'Servicios especiales',
    '50': 'Contrato mixto', '999': 'Otros',
}


def get_db():
    conn = sqlite3.connect(RUTA_DB, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')  # permite leer mientras se escribe
    return conn


def tipo_label(code):
    if not code:
        return '—'
    return MAP_TIPO_CONTRATO.get(str(code), f'Tipo {code}')


def _fts_disponible(conn):
    """Comprueba si la tabla FTS ya tiene datos."""
    try:
        r = conn.execute("SELECT COUNT(*) FROM textos_pdf WHERE n_chars > 50").fetchone()
        return r[0] > 0
    except Exception:
        return False


# ─────────────────────────── CSS ────────────────────────────────────────────

CSS = '''
* { box-sizing: border-box; }
body { font-family: Arial, sans-serif; margin: 0; background: #f0f2f5; color: #222; }
.topbar { background: #c00; color: white; padding: 10px 24px; display:flex; align-items:center; gap:20px; }
.topbar h1 { margin:0; font-size:20px; flex:1; }
.topbar a { color: rgba(255,255,255,.85); text-decoration:none; font-size:13px; padding:4px 10px;
            border-radius:4px; transition:background .2s; }
.topbar a:hover, .topbar a.active { background:rgba(255,255,255,.2); color:white; }
.container { max-width: 1200px; margin: 0 auto; padding: 18px 16px; }
h2 { color: #333; font-size:16px; margin:22px 0 10px; border-bottom:2px solid #e0e0e0; padding-bottom:5px; }

/* Buscador */
.card { background:white; padding:16px 20px; border-radius:8px; margin-bottom:14px;
        box-shadow:0 2px 6px rgba(0,0,0,.08); }
.buscador input, .buscador select {
  padding:7px 10px; margin:3px; border:1px solid #ddd; border-radius:4px; font-size:13px; }
.buscador .row { display:flex; flex-wrap:wrap; gap:4px; align-items:center; margin-bottom:6px; }
.btn-buscar { padding:7px 18px; background:#c00; color:white; border:none; border-radius:4px;
              cursor:pointer; font-size:13px; }
.btn-buscar:hover { background:#a00; }
.limpiar { margin-left:6px; color:#666; font-size:13px; text-decoration:none; }
.btn-export { padding:6px 14px; background:#1565c0; color:white; border:none; border-radius:4px;
              cursor:pointer; font-size:12px; text-decoration:none; display:inline-block; }

/* Resultados */
.resultado { background:white; padding:14px 16px; margin-bottom:7px; border-radius:8px;
             box-shadow:0 2px 4px rgba(0,0,0,.07); }
.resultado h3 { margin:0 0 5px; color:#333; font-size:14px; line-height:1.4; }
.tag { display:inline-block; padding:2px 8px; border-radius:12px; font-size:11px; margin-right:4px; }
.tag-estado { background:#e3f2fd; color:#1565c0; }
.tag-fuente { background:#f3e5f5; color:#6a1b9a; }
.tag-tipo   { background:#e8f5e9; color:#2e7d32; }
.tag-pdf    { background:#fff3e0; color:#e65100; }
.importe    { font-weight:bold; color:#c00; font-size:14px; }
.meta       { color:#666; font-size:12px; margin-top:3px; }
.botones    { margin-top:7px; }
.btn { padding:4px 11px; border-radius:4px; text-decoration:none; font-size:12px;
       margin-right:4px; display:inline-block; }
.btn-ver    { background:#1565c0; color:white; }
.btn-doc    { background:#2e7d32; color:white; }
.btn-url    { background:#e65100; color:white; }
.btn-enlace { background:#555; color:white; }
.total { color:#666; margin-bottom:10px; font-size:13px; display:flex; align-items:center; gap:10px; }
.paginacion a { color:#c00; text-decoration:none; }

/* Snippet FTS */
.snippet { background:#fffde7; border-left:3px solid #f9a825; padding:6px 10px;
           font-size:12px; color:#555; margin-top:5px; border-radius:0 4px 4px 0; }
.snippet b { color:#c00; }
.fts-badge { background:#f57f17; color:white; font-size:11px; padding:2px 7px;
             border-radius:10px; margin-left:6px; }

/* Ficha */
.ficha table { width:100%; border-collapse:collapse; }
.ficha td { padding:7px 10px; border-bottom:1px solid #f0f0f0; font-size:13px; vertical-align:top; }
.ficha td:first-child { font-weight:bold; color:#666; width:210px; white-space:nowrap; }
.mini-table { width:100%; border-collapse:collapse; font-size:13px; }
.mini-table th { background:#f5f5f5; padding:6px 10px; text-align:left; border-bottom:2px solid #ddd; }
.mini-table td { padding:6px 10px; border-bottom:1px solid #eee; }
.volver { display:inline-block; margin-bottom:12px; color:#c00; text-decoration:none; font-size:13px; }
.tag-estado-lg { display:inline-block; padding:3px 12px; border-radius:12px; font-size:14px;
                 background:#e3f2fd; color:#1565c0; }
.texto-pliego { background:#fafafa; border:1px solid #e0e0e0; border-radius:6px;
                padding:14px; font-size:12px; font-family:monospace; white-space:pre-wrap;
                max-height:500px; overflow-y:auto; line-height:1.5; }
.tab-btns { display:flex; gap:6px; margin-bottom:10px; }
.tab-btn { padding:5px 14px; border:1px solid #ddd; border-radius:4px; background:white;
           cursor:pointer; font-size:13px; color:#555; }
.tab-btn.active { background:#c00; color:white; border-color:#c00; }

/* Dashboard */
.stats-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(170px,1fr)); gap:14px; margin-bottom:20px; }
.stat-card { background:white; border-radius:8px; padding:16px 18px;
             box-shadow:0 2px 6px rgba(0,0,0,.08); text-align:center; }
.stat-card .num { font-size:28px; font-weight:bold; color:#c00; }
.stat-card .lbl { font-size:12px; color:#888; margin-top:4px; }
.bar-chart { width:100%; }
.bar-row { display:flex; align-items:center; margin-bottom:5px; font-size:12px; }
.bar-row .label { width:200px; text-align:right; padding-right:8px; color:#555; white-space:nowrap;
                  overflow:hidden; text-overflow:ellipsis; }
.bar-row .bar { height:16px; background:#c00; border-radius:2px; min-width:2px; }
.bar-row .val { margin-left:6px; color:#666; }
'''

# ─────────────────────────── TEMPLATES ──────────────────────────────────────

TOPBAR = '''
<div class="topbar">
  <h1>📋 Repositorio de Licitaciones</h1>
  <a href="/" class="{{ 'active' if active=='buscar' else '' }}">🔍 Buscador</a>
  <a href="/pliegos" class="{{ 'active' if active=='pliegos' else '' }}">📄 Buscar en pliegos</a>
  <a href="/dashboard" class="{{ 'active' if active=='dashboard' else '' }}">📊 Dashboard</a>
</div>
'''

TEMPLATE_BUSCADOR = '''
<!DOCTYPE html><html lang="es"><head><meta charset="UTF-8"><title>Repositorio Licitaciones</title>
<style>{{ css }}</style></head><body>
''' + TOPBAR + '''
<div class="container">
<div class="card buscador">
  <form method="GET" action="/" id="form-buscar">
    <div class="row">
      <input type="text" name="q" placeholder="Descripción, órgano o expediente..."
             value="{{ q }}" style="width:300px;">
      <select name="ccaa">
        <option value="">Todas las CCAA</option>
        {% for c in ccaas %}<option value="{{ c }}" {% if c==ccaa_sel %}selected{% endif %}>{{ c }}</option>{% endfor %}
      </select>
      <select name="tipo">
        <option value="">Todos los tipos</option>
        {% for code,label in tipos %}<option value="{{ code }}" {% if code==tipo_sel %}selected{% endif %}>{{ label }}</option>{% endfor %}
      </select>
      <select name="estado">
        <option value="">Todos los estados</option>
        {% for e in estados %}<option value="{{ e }}" {% if e==estado_sel %}selected{% endif %}>{{ e }}</option>{% endfor %}
      </select>
      <select name="fuente">
        <option value="">Todas las fuentes</option>
        {% for f in fuentes %}<option value="{{ f }}" {% if f==fuente_sel %}selected{% endif %}>{{ f }}</option>{% endfor %}
      </select>
    </div>
    <div class="row">
      <input type="number" name="imp_min" placeholder="Importe mín. €" value="{{ imp_min }}" style="width:140px;">
      <input type="number" name="imp_max" placeholder="Importe máx. €" value="{{ imp_max }}" style="width:140px;">
      <label style="font-size:13px;margin-left:8px;">
        <input type="checkbox" name="con_pdf" value="1" {% if con_pdf %}checked{% endif %}> Solo con PDF descargado
      </label>
      <button class="btn-buscar" type="submit">Buscar</button>
      <a class="limpiar" href="/">Limpiar</a>
    </div>
  </form>
</div>

<div class="total">
  <span>{{ total }} licitaciones encontradas</span>
  {% if total > 0 %}
    <a class="btn-export" href="/exportar?{{ query_string }}">⬇ Exportar CSV</a>
  {% endif %}
  <span style="flex:1"></span>
  {% if total > page_size %}
    {% if offset > 0 %}<a class="paginacion" href="{{ prev_url }}">← Anterior</a>{% endif %}
    <span>{{ offset+1 }}–{{ [offset+page_size, total]|min }} de {{ total }}</span>
    {% if offset+page_size < total %}<a class="paginacion" href="{{ next_url }}">Siguiente →</a>{% endif %}
  {% endif %}
</div>

{% for l in licitaciones %}
<div class="resultado">
  <h3>{{ (l.objeto_contrato or l.titulo or '')[:140] }}{% if (l.objeto_contrato or l.titulo or '')|length > 140 %}...{% endif %}</h3>
  <div>
    <span class="tag tag-estado">{{ l.estado }}</span>
    <span class="tag tag-fuente">{{ l.fuente }}</span>
    <span class="tag tag-tipo">{{ tipo_label(l.tipo_contrato_code) }}</span>
    {% if l.tiene_pdf %}<span class="tag tag-pdf">📄 PDF</span>{% endif %}
  </div>
  <div class="meta">{{ l.org_nombre }} · {{ l.org_ciudad }}{% if l.ccaa %} ({{ l.ccaa }}){% endif %} · CPV: {{ l.cpv_principal }}</div>
  <div class="meta">Límite: {{ l.fecha_limite_presentacion or '—' }} · {{ l.procedimiento or '' }}</div>
  {% if l.importe_licitacion_sin_iva %}<div class="importe">{{ "{:,.2f}".format(l.importe_licitacion_sin_iva) }} € sin IVA</div>{% endif %}
  <div class="botones">
    <a class="btn btn-ver" href="/licitacion/{{ l.expediente | urlencode }}">Ver ficha</a>
    {% if l.entry_link and l.entry_link not in ('', 'Sin dato') %}
      <a class="btn btn-enlace" href="{{ l.entry_link }}" target="_blank">PLACSP ↗</a>
    {% endif %}
  </div>
</div>
{% else %}
<div style="text-align:center;padding:40px;color:#888;">No se encontraron licitaciones.</div>
{% endfor %}
</div></body></html>
'''

TEMPLATE_PLIEGOS = '''
<!DOCTYPE html><html lang="es"><head><meta charset="UTF-8"><title>Buscar en pliegos</title>
<style>{{ css }}</style></head><body>
''' + TOPBAR + '''
<div class="container">
<div class="card buscador">
  <form method="GET" action="/pliegos">
    <div class="row">
      <input type="text" name="q" placeholder="Buscar en el texto de pliegos... (ej: solvencia económica, plazo ejecución)"
             value="{{ q }}" style="width:450px;">
      <select name="tipo_doc">
        <option value="">Todos los pliegos</option>
        <option value="pliego_administrativo" {% if tipo_doc=='pliego_administrativo' %}selected{% endif %}>Solo PCAP (admin)</option>
        <option value="pliego_tecnico" {% if tipo_doc=='pliego_tecnico' %}selected{% endif %}>Solo PPT (técnico)</option>
      </select>
      <button class="btn-buscar" type="submit">Buscar en texto</button>
      <a class="limpiar" href="/pliegos">Limpiar</a>
    </div>
  </form>
</div>

{% if not fts_ok %}
<div class="card" style="color:#888;text-align:center;padding:30px;">
  ⏳ La extracción de texto está en curso. Vuelve en unos minutos.
</div>
{% elif not q %}
<div class="card" style="color:#888;text-align:center;padding:30px;">
  <b>{{ total_indexados }}</b> pliegos indexados con texto completo.<br><br>
  Escribe cualquier término para buscar dentro del contenido de los pliegos.<br>
  <small>Ejemplos: <i>solvencia mínima</i> · <i>criterios precio</i> · <i>plazo garantía</i> · <i>subcontratación</i></small>
</div>
{% else %}
<div class="total">
  <span>{{ resultados|length }} pliegos con «{{ q }}»
    {% if resultados|length == 50 %}<span class="fts-badge">top 50</span>{% endif %}
  </span>
</div>
{% for r in resultados %}
<div class="resultado">
  <h3>
    <a href="/licitacion/{{ r.expediente | urlencode }}" style="color:#1565c0;text-decoration:none;">
      {{ r.expediente }}
    </a>
    <span class="tag tag-pdf" style="margin-left:6px;">{{ r.tipo_documento }}</span>
  </h3>
  <div class="meta">{{ r.objeto or '' }} · {{ r.org_nombre or '' }}{% if r.ccaa %} ({{ r.ccaa }}){% endif %}</div>
  {% if r.importe %}
    <div class="importe" style="font-size:13px;">{{ "{:,.2f}".format(r.importe) }} €</div>
  {% endif %}
  <div class="snippet">{{ r.snippet | safe }}</div>
  <div class="botones" style="margin-top:6px;">
    <a class="btn btn-ver" href="/licitacion/{{ r.expediente | urlencode }}">Ver ficha</a>
    <a class="btn btn-doc" href="/pliego/{{ r.doc_id }}?resaltar={{ q | urlencode }}" target="_blank">Ver pliego completo</a>
  </div>
</div>
{% else %}
<div style="text-align:center;padding:40px;color:#888;">No se encontraron pliegos con ese término.</div>
{% endfor %}
{% endif %}
</div></body></html>
'''

TEMPLATE_FICHA = '''
<!DOCTYPE html><html lang="es"><head><meta charset="UTF-8">
<title>{{ (l.objeto_contrato or l.titulo or l.expediente)[:60] }}</title>
<style>{{ css }}</style></head><body>
''' + TOPBAR + '''
<div class="container">
<a class="volver" href="javascript:history.back()">← Volver</a>
<h2 style="font-size:18px;border:none;margin-bottom:12px;">{{ l.objeto_contrato or l.titulo or l.expediente }}</h2>

<div class="card ficha">
  <table>
    <tr><td>Expediente</td><td><strong>{{ l.expediente }}</strong></td></tr>
    <tr><td>Estado</td><td><span class="tag-estado-lg">{{ l.estado }}</span></td></tr>
    <tr><td>Fuente</td><td>{{ l.fuente }}</td></tr>
    <tr><td>Periodo</td><td>{{ l.periodo }}</td></tr>
    <tr><td>Órgano contratante</td><td>{{ l.org_nombre }}{% if l.org_nif %} (NIF: {{ l.org_nif }}){% endif %}</td></tr>
    <tr><td>Localidad</td><td>{{ l.org_ciudad }}{% if l.org_provincia %}, {{ l.org_provincia }}{% endif %}{% if l.ccaa %} — {{ l.ccaa }}{% endif %}</td></tr>
    <tr><td>Tipo de contrato</td><td>{{ tipo_label(l.tipo_contrato_code) }}{% if l.subtipo_contrato_code %} / subtipo {{ l.subtipo_contrato_code }}{% endif %}</td></tr>
    <tr><td>CPV principal</td><td>{{ l.cpv_principal }}</td></tr>
    {% if l.cpvs_adicionales %}<tr><td>CPVs adicionales</td><td>{{ l.cpvs_adicionales }}</td></tr>{% endif %}
    <tr><td>Procedimiento</td><td>{{ l.procedimiento }}</td></tr>
    <tr><td>Tramitación</td><td>{{ l.tramitacion }}</td></tr>
    {% if l.importe_licitacion_sin_iva %}<tr><td>Importe licitación (sin IVA)</td><td><strong>{{ "{:,.2f}".format(l.importe_licitacion_sin_iva) }} €</strong></td></tr>{% endif %}
    {% if l.importe_licitacion_con_iva %}<tr><td>Importe licitación (con IVA)</td><td>{{ "{:,.2f}".format(l.importe_licitacion_con_iva) }} €</td></tr>{% endif %}
    {% if l.presupuesto_estimado %}<tr><td>Presupuesto estimado</td><td>{{ "{:,.2f}".format(l.presupuesto_estimado) }} €</td></tr>{% endif %}
    <tr><td>Fecha límite presentación</td><td>{{ l.fecha_limite_presentacion }}{% if l.hora_limite_presentacion %} {{ l.hora_limite_presentacion }}{% endif %}</td></tr>
    <tr><td>Duración contrato</td><td>{{ l.duracion_contrato }} {{ l.duracion_unidad }}</td></tr>
    <tr><td>Num. lotes</td><td>{{ l.num_lotes }}</td></tr>
    {% if l.financiacion %}<tr><td>Financiación UE</td><td>{{ l.financiacion }}</td></tr>{% endif %}
    {% if l.garantia_tipo %}<tr><td>Garantía</td><td>{{ l.garantia_tipo }}{% if l.garantia_porcentaje %} — {{ l.garantia_porcentaje }}%{% endif %}</td></tr>{% endif %}
    {% if l.solvencia_economica %}<tr><td>Solvencia económica</td><td>{{ l.solvencia_economica }}</td></tr>{% endif %}
    {% if l.solvencia_tecnica %}<tr><td>Solvencia técnica</td><td>{{ l.solvencia_tecnica }}</td></tr>{% endif %}
    {% if l.adjudicatario_nombre %}<tr><td>Adjudicatario</td><td>{{ l.adjudicatario_nombre }}{% if l.adjudicatario_nif %} ({{ l.adjudicatario_nif }}){% endif %}</td></tr>{% endif %}
    {% if l.importe_adjudicacion %}<tr><td>Importe adjudicación</td><td>{{ "{:,.2f}".format(l.importe_adjudicacion) }} €</td></tr>{% endif %}
    {% if l.fecha_adjudicacion %}<tr><td>Fecha adjudicación</td><td>{{ l.fecha_adjudicacion }}</td></tr>{% endif %}
    {% if l.num_ofertas_recibidas %}<tr><td>Ofertas recibidas</td><td>{{ l.num_ofertas_recibidas }}</td></tr>{% endif %}
  </table>

  <div style="margin-top:14px;">
    {% if l.entry_link and l.entry_link not in ('', 'Sin dato') %}
      <a class="btn btn-enlace" href="{{ l.entry_link }}" target="_blank">Ver en PLACSP ↗</a>
    {% endif %}
    {% if l.buyer_profile and l.buyer_profile not in ('', 'Sin dato') %}
      <a class="btn btn-enlace" href="{{ l.buyer_profile }}" target="_blank">Perfil contratante ↗</a>
    {% endif %}
  </div>
</div>

{% if lotes %}
<div class="card">
  <h2>Lotes ({{ lotes|length }})</h2>
  <table class="mini-table">
    <tr><th>ID</th><th>Descripción</th><th>CPV</th><th>Importe</th><th>Adjudicatario</th><th>Importe adj.</th></tr>
    {% for lot in lotes %}
    <tr>
      <td>{{ lot.lote_id }}</td>
      <td>{{ lot.objeto_lote }}</td>
      <td>{{ lot.cpv_lote }}</td>
      <td>{% if lot.importe_lote %}{{ "{:,.2f}".format(lot.importe_lote) }} €{% endif %}</td>
      <td>{{ lot.adjudicatario_nombre }}</td>
      <td>{% if lot.importe_adjudicacion %}{{ "{:,.2f}".format(lot.importe_adjudicacion) }} €{% endif %}</td>
    </tr>
    {% endfor %}
  </table>
</div>
{% endif %}

{% if criterios %}
<div class="card">
  <h2>Criterios de adjudicación ({{ criterios|length }})</h2>
  <table class="mini-table">
    <tr><th>Lote</th><th>Tipo</th><th>Descripción</th><th>Peso</th></tr>
    {% for c in criterios %}
    <tr>
      <td>{{ c.lote_id or '—' }}</td>
      <td>{{ c.tipo_criterio }}</td>
      <td>{{ c.descripcion }}</td>
      <td>{{ c.peso }}</td>
    </tr>
    {% endfor %}
  </table>
</div>
{% endif %}

{% if documentos %}
<div class="card">
  <h2>Documentos ({{ documentos|length }})</h2>
  <table class="mini-table">
    <tr><th>Tipo</th><th>ID</th><th>Fecha pub.</th><th>Medio</th><th>Acceso</th></tr>
    {% for d in documentos %}
    <tr>
      <td>{{ d.tipo_documento }}</td>
      <td>{{ d.doc_id }}</td>
      <td>{{ d.fecha_publicacion }}</td>
      <td>{{ d.medio_publicacion }}</td>
      <td>
        {% if d.descargado and d.ruta_local %}
          <a class="btn btn-doc" href="/doc/{{ d.id }}" target="_blank">PDF local</a>
        {% endif %}
        {% if d.doc_url and d.doc_url not in ('', 'Sin dato') %}
          <a class="btn btn-url" href="{{ d.doc_url }}" target="_blank">URL ↗</a>
        {% endif %}
      </td>
    </tr>
    {% endfor %}
  </table>
</div>
{% endif %}

{% if textos_pliego %}
<div class="card">
  <h2>Texto de pliegos extraído</h2>
  <div class="tab-btns" id="tab-btns">
    {% for tp in textos_pliego %}
    <button class="tab-btn {% if loop.first %}active{% endif %}"
            onclick="mostrarTab({{ loop.index0 }})">
      {{ tp.tipo_documento }} ({{ tp.n_paginas }} pág · {{ '{:,}'.format(tp.n_chars) }} chars)
    </button>
    {% endfor %}
  </div>
  {% for tp in textos_pliego %}
  <div id="tab-{{ loop.index0 }}" class="texto-pliego" {% if not loop.first %}style="display:none"{% endif %}>{{ tp.texto_raw[:15000] }}{% if tp.texto_raw|length > 15000 %}

... [texto truncado — {{ '{:,}'.format(tp.n_chars) }} chars totales] ...{% endif %}
  </div>
  {% endfor %}
</div>
<script>
function mostrarTab(i) {
  document.querySelectorAll('.texto-pliego').forEach((el,j) => el.style.display = j==i?'':'none');
  document.querySelectorAll('.tab-btn').forEach((el,j) => el.classList.toggle('active', j==i));
}
</script>
{% endif %}

</div></body></html>
'''

TEMPLATE_DASHBOARD = '''
<!DOCTYPE html><html lang="es"><head><meta charset="UTF-8"><title>Dashboard</title>
<style>{{ css }}</style></head><body>
''' + TOPBAR + '''
<div class="container">
<h2 style="border:none;font-size:20px;margin-bottom:16px;">Dashboard</h2>

<div class="stats-grid">
  <div class="stat-card"><div class="num">{{ stats.total_lics }}</div><div class="lbl">Licitaciones</div></div>
  <div class="stat-card"><div class="num">{{ stats.total_docs }}</div><div class="lbl">Documentos</div></div>
  <div class="stat-card"><div class="num">{{ stats.total_pdfs }}</div><div class="lbl">PDFs descargados</div></div>
  <div class="stat-card"><div class="num">{{ stats.total_pliegos_txt }}</div><div class="lbl">Pliegos con texto</div></div>
  <div class="stat-card"><div class="num">{{ "{:,.0f}".format(stats.importe_total / 1e6) }} M€</div><div class="lbl">Importe total licitado</div></div>
  <div class="stat-card"><div class="num">{{ stats.periodos }}</div><div class="lbl">Periodos indexados</div></div>
</div>

<div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;">

<div class="card">
  <h2>Licitaciones por CCAA (top 12)</h2>
  <div class="bar-chart">
    {% set max_val = por_ccaa[0][1] if por_ccaa else 1 %}
    {% for ccaa, n in por_ccaa %}
    <div class="bar-row">
      <div class="label" title="{{ ccaa }}">{{ ccaa or '(sin CCAA)' }}</div>
      <div class="bar" style="width:{{ (n/max_val*220)|int }}px"></div>
      <div class="val">{{ n }}</div>
    </div>
    {% endfor %}
  </div>
</div>

<div class="card">
  <h2>Licitaciones por tipo de contrato</h2>
  <div class="bar-chart">
    {% set max_val2 = por_tipo[0][2] if por_tipo else 1 %}
    {% for code, label, n in por_tipo %}
    <div class="bar-row">
      <div class="label" title="{{ label }}">{{ label }}</div>
      <div class="bar" style="width:{{ (n/max_val2*220)|int }}px"></div>
      <div class="val">{{ n }}</div>
    </div>
    {% endfor %}
  </div>
</div>

<div class="card">
  <h2>Top 10 órganos por importe licitado</h2>
  <table class="mini-table">
    <tr><th>Órgano</th><th style="text-align:right">Importe</th></tr>
    {% for org, imp in top_organos %}
    <tr>
      <td>{{ org[:55] }}{% if org|length > 55 %}...{% endif %}</td>
      <td style="text-align:right;font-weight:bold;color:#c00;">{{ "{:,.0f}".format(imp/1e6) }} M€</td>
    </tr>
    {% endfor %}
  </table>
</div>

<div class="card">
  <h2>Licitaciones por mes (últimos 24)</h2>
  <div class="bar-chart">
    {% set max_val3 = por_mes[0][1] if por_mes else 1 %}
    {% for mes, n in por_mes %}
    <div class="bar-row">
      <div class="label">{{ mes[:4] }}/{{ mes[4:] }}</div>
      <div class="bar" style="width:{{ (n/max_val3*220)|int }}px;background:#1565c0"></div>
      <div class="val">{{ n }}</div>
    </div>
    {% endfor %}
  </div>
</div>

</div>
</div></body></html>
'''

TEMPLATE_PLIEGO_TEXTO = '''
<!DOCTYPE html><html lang="es"><head><meta charset="UTF-8">
<title>{{ tipo_documento }} — {{ expediente }}</title>
<style>
body { font-family: Arial, sans-serif; margin: 20px; background:#fafafa; }
h2 { color:#333; }
.meta { color:#888; font-size:13px; margin-bottom:16px; }
.texto { background:white; border:1px solid #ddd; border-radius:6px; padding:20px;
         font-size:13px; font-family: Georgia, serif; white-space:pre-wrap; line-height:1.7;
         max-width: 900px; }
.resaltado { background:#ffeb3b; }
.volver { color:#c00; font-size:13px; text-decoration:none; }
</style>
</head><body>
<a class="volver" href="javascript:history.back()">← Volver</a>
<h2>{{ tipo_documento }} — Expediente {{ expediente }}</h2>
<div class="meta">{{ n_paginas }} páginas · {{ "{:,}".format(n_chars) }} caracteres</div>
<div class="texto" id="texto">{{ texto | e }}</div>
{% if resaltar %}
<script>
(function(){
  var el = document.getElementById('texto');
  var term = {{ resaltar | tojson }};
  var re = new RegExp(term.replace(/[.*+?^${}()|[\]\\]/g,'\\$&'), 'gi');
  el.innerHTML = el.innerHTML.replace(re, m => '<mark class="resaltado">'+m+'</mark>');
})();
</script>
{% endif %}
</body></html>
'''


# ─────────────────────────── RUTAS ──────────────────────────────────────────

@app.route('/')
def buscador():
    q          = request.args.get('q', '')
    ccaa_sel   = request.args.get('ccaa', '')
    tipo_sel   = request.args.get('tipo', '')
    estado_sel = request.args.get('estado', '')
    fuente_sel = request.args.get('fuente', '')
    imp_min    = request.args.get('imp_min', '')
    imp_max    = request.args.get('imp_max', '')
    con_pdf    = request.args.get('con_pdf', '')
    try:
        offset = int(request.args.get('offset', 0))
    except ValueError:
        offset = 0
    page_size = 100

    conn = get_db()
    ccaas   = [r[0] for r in conn.execute("SELECT DISTINCT ccaa FROM licitaciones WHERE ccaa NOT IN ('','Sin dato') ORDER BY ccaa").fetchall()]
    estados = [r[0] for r in conn.execute("SELECT DISTINCT estado FROM licitaciones WHERE estado NOT IN ('','Sin dato') ORDER BY estado").fetchall()]
    fuentes = [r[0] for r in conn.execute("SELECT DISTINCT fuente FROM licitaciones WHERE fuente != '' ORDER BY fuente").fetchall()]
    tipos_raw = [r[0] for r in conn.execute("SELECT DISTINCT tipo_contrato_code FROM licitaciones WHERE tipo_contrato_code != '' ORDER BY tipo_contrato_code").fetchall()]
    tipos = [(c, tipo_label(c)) for c in tipos_raw]

    where = ['1=1']
    params = []
    if q:
        where.append('(objeto_contrato LIKE ? OR titulo LIKE ? OR org_nombre LIKE ? OR expediente LIKE ?)')
        params += [f'%{q}%'] * 4
    if ccaa_sel:
        where.append('ccaa = ?')
        params.append(ccaa_sel)
    if tipo_sel:
        where.append('tipo_contrato_code = ?')
        params.append(tipo_sel)
    if estado_sel:
        where.append('estado = ?')
        params.append(estado_sel)
    if fuente_sel:
        where.append('fuente = ?')
        params.append(fuente_sel)
    if imp_min:
        where.append('importe_licitacion_sin_iva >= ?')
        params.append(float(imp_min))
    if imp_max:
        where.append('importe_licitacion_sin_iva <= ?')
        params.append(float(imp_max))
    if con_pdf:
        where.append("expediente IN (SELECT DISTINCT expediente FROM documentos WHERE descargado=1)")

    wc = ' AND '.join(where)

    # Añadir columna virtual tiene_pdf
    total = conn.execute(f'SELECT COUNT(*) FROM licitaciones WHERE {wc}', params).fetchone()[0]
    rows = conn.execute(f'''
        SELECT l.*,
          CASE WHEN EXISTS(SELECT 1 FROM documentos d WHERE d.expediente=l.expediente AND d.descargado=1) THEN 1 ELSE 0 END AS tiene_pdf
        FROM licitaciones l
        WHERE {wc}
        ORDER BY fecha_limite_presentacion DESC
        LIMIT ? OFFSET ?
    ''', params + [page_size, offset]).fetchall()
    conn.close()

    def _url(new_offset):
        args = request.args.copy()
        args['offset'] = new_offset
        return '/?' + '&'.join(f'{k}={v}' for k, v in args.items(multi=True))

    qs = '&'.join(f'{k}={v}' for k, v in request.args.items(multi=True) if k != 'offset')

    return render_template_string(
        TEMPLATE_BUSCADOR,
        css=CSS, licitaciones=rows, total=total,
        ccaas=ccaas, tipos=tipos, estados=estados, fuentes=fuentes,
        q=q, ccaa_sel=ccaa_sel, tipo_sel=tipo_sel, estado_sel=estado_sel,
        fuente_sel=fuente_sel, imp_min=imp_min, imp_max=imp_max, con_pdf=con_pdf,
        offset=offset, page_size=page_size,
        prev_url=_url(max(0, offset - page_size)),
        next_url=_url(offset + page_size),
        query_string=qs,
        tipo_label=tipo_label,
        active='buscar',
    )


@app.route('/exportar')
def exportar():
    """Exporta los resultados de búsqueda actuales a CSV."""
    q          = request.args.get('q', '')
    ccaa_sel   = request.args.get('ccaa', '')
    tipo_sel   = request.args.get('tipo', '')
    estado_sel = request.args.get('estado', '')
    fuente_sel = request.args.get('fuente', '')
    imp_min    = request.args.get('imp_min', '')
    imp_max    = request.args.get('imp_max', '')
    con_pdf    = request.args.get('con_pdf', '')

    where = ['1=1']
    params = []
    if q:
        where.append('(objeto_contrato LIKE ? OR titulo LIKE ? OR org_nombre LIKE ? OR expediente LIKE ?)')
        params += [f'%{q}%'] * 4
    if ccaa_sel:
        where.append('ccaa = ?')
        params.append(ccaa_sel)
    if tipo_sel:
        where.append('tipo_contrato_code = ?')
        params.append(tipo_sel)
    if estado_sel:
        where.append('estado = ?')
        params.append(estado_sel)
    if fuente_sel:
        where.append('fuente = ?')
        params.append(fuente_sel)
    if imp_min:
        where.append('importe_licitacion_sin_iva >= ?')
        params.append(float(imp_min))
    if imp_max:
        where.append('importe_licitacion_sin_iva <= ?')
        params.append(float(imp_max))
    if con_pdf:
        where.append("expediente IN (SELECT DISTINCT expediente FROM documentos WHERE descargado=1)")

    wc = ' AND '.join(where)
    conn = sqlite3.connect(RUTA_DB, timeout=30)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(f'SELECT * FROM licitaciones WHERE {wc} ORDER BY fecha_limite_presentacion DESC LIMIT 10000', params).fetchall()
    conn.close()

    output = io.StringIO()
    if rows:
        writer = csv.DictWriter(output, fieldnames=rows[0].keys(), delimiter=';',
                                quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows([dict(r) for r in rows])

    response = make_response(output.getvalue().encode('utf-8-sig'))
    response.headers['Content-Type'] = 'text/csv; charset=utf-8-sig'
    response.headers['Content-Disposition'] = 'attachment; filename=licitaciones_export.csv'
    return response


@app.route('/pliegos')
def buscar_pliegos():
    q        = request.args.get('q', '').strip()
    tipo_doc = request.args.get('tipo_doc', '')

    conn = get_db()
    fts_ok = _fts_disponible(conn)

    total_indexados = 0
    resultados = []

    if fts_ok:
        total_indexados = conn.execute("SELECT COUNT(*) FROM textos_pdf WHERE n_chars > 50").fetchone()[0]

    if fts_ok and q:
        tipo_filter = f"AND t.tipo_documento = '{tipo_doc}'" if tipo_doc else ''
        try:
            rows = conn.execute(f'''
                SELECT
                    f.rowid,
                    t.doc_id,
                    t.expediente,
                    t.tipo_documento,
                    t.n_paginas,
                    t.n_chars,
                    snippet(textos_pdf_fts, 2, "<b>", "</b>", " … ", 30) AS snip,
                    l.objeto_contrato,
                    l.org_nombre,
                    l.ccaa,
                    l.importe_licitacion_sin_iva
                FROM textos_pdf_fts f
                JOIN textos_pdf t ON t.id = f.rowid
                LEFT JOIN licitaciones l ON l.expediente = t.expediente
                WHERE textos_pdf_fts MATCH ?
                  {tipo_filter}
                ORDER BY rank
                LIMIT 50
            ''', (q,)).fetchall()

            for r in rows:
                resultados.append({
                    'doc_id':         r['doc_id'],
                    'expediente':     r['expediente'],
                    'tipo_documento': r['tipo_documento'],
                    'objeto':         r['objeto_contrato'] or '',
                    'org_nombre':     r['org_nombre'] or '',
                    'ccaa':           r['ccaa'] or '',
                    'importe':        r['importe_licitacion_sin_iva'],
                    'snippet':        r['snip'] or '',
                })
        except Exception as e:
            resultados = []

    conn.close()
    return render_template_string(
        TEMPLATE_PLIEGOS,
        css=CSS, q=q, tipo_doc=tipo_doc,
        fts_ok=fts_ok, total_indexados=total_indexados,
        resultados=resultados,
        active='pliegos',
    )


@app.route('/pliego/<int:doc_id>')
def ver_pliego_texto(doc_id):
    resaltar = request.args.get('resaltar', '')
    conn = get_db()
    row = conn.execute('''
        SELECT t.expediente, t.tipo_documento, t.n_paginas, t.n_chars, t.texto_raw
        FROM textos_pdf t WHERE t.doc_id = ?
    ''', (doc_id,)).fetchone()
    conn.close()
    if not row or not row['texto_raw']:
        abort(404)
    return render_template_string(
        TEMPLATE_PLIEGO_TEXTO,
        expediente=row['expediente'],
        tipo_documento=row['tipo_documento'],
        n_paginas=row['n_paginas'],
        n_chars=row['n_chars'],
        texto=row['texto_raw'],
        resaltar=resaltar,
    )


@app.route('/dashboard')
def dashboard():
    conn = get_db()

    stats = {}
    stats['total_lics']       = conn.execute("SELECT COUNT(*) FROM licitaciones").fetchone()[0]
    stats['total_docs']       = conn.execute("SELECT COUNT(*) FROM documentos").fetchone()[0]
    stats['total_pdfs']       = conn.execute("SELECT COUNT(*) FROM documentos WHERE descargado=1").fetchone()[0]
    stats['periodos']         = conn.execute("SELECT COUNT(DISTINCT periodo) FROM licitaciones").fetchone()[0]
    stats['importe_total']    = conn.execute("SELECT COALESCE(SUM(importe_licitacion_sin_iva),0) FROM licitaciones WHERE importe_licitacion_sin_iva > 0").fetchone()[0]
    try:
        stats['total_pliegos_txt'] = conn.execute("SELECT COUNT(*) FROM textos_pdf WHERE n_chars > 50").fetchone()[0]
    except Exception:
        stats['total_pliegos_txt'] = 0

    por_ccaa = conn.execute("""
        SELECT COALESCE(NULLIF(ccaa,''),'(sin CCAA)'), COUNT(*) n
        FROM licitaciones GROUP BY ccaa ORDER BY n DESC LIMIT 12
    """).fetchall()

    por_tipo_raw = conn.execute("""
        SELECT tipo_contrato_code, COUNT(*) n
        FROM licitaciones WHERE tipo_contrato_code != ''
        GROUP BY tipo_contrato_code ORDER BY n DESC LIMIT 10
    """).fetchall()
    por_tipo = [(r[0], tipo_label(r[0]), r[1]) for r in por_tipo_raw]

    top_organos = conn.execute("""
        SELECT org_nombre, SUM(importe_licitacion_sin_iva) t
        FROM licitaciones WHERE importe_licitacion_sin_iva > 0
        GROUP BY org_nombre ORDER BY t DESC LIMIT 10
    """).fetchall()

    por_mes = conn.execute("""
        SELECT periodo, COUNT(*) n FROM licitaciones
        GROUP BY periodo ORDER BY periodo DESC LIMIT 24
    """).fetchall()
    por_mes = list(reversed(por_mes))

    conn.close()
    return render_template_string(
        TEMPLATE_DASHBOARD,
        css=CSS, stats=stats,
        por_ccaa=por_ccaa, por_tipo=por_tipo,
        top_organos=top_organos, por_mes=por_mes,
        active='dashboard',
    )


@app.route('/licitacion/<expediente>')
def ficha(expediente):
    conn = get_db()
    l = conn.execute('SELECT * FROM licitaciones WHERE expediente = ?', (expediente,)).fetchone()
    if not l:
        conn.close()
        abort(404)
    lotes      = conn.execute('SELECT * FROM lotes WHERE expediente = ? ORDER BY lote_id', (expediente,)).fetchall()
    criterios  = conn.execute('SELECT * FROM criterios WHERE expediente = ? ORDER BY lote_id, tipo_criterio', (expediente,)).fetchall()
    documentos = conn.execute('SELECT * FROM documentos WHERE expediente = ? ORDER BY tipo_documento, fecha_publicacion', (expediente,)).fetchall()
    textos_pliego = conn.execute('''
        SELECT tipo_documento, n_paginas, n_chars, texto_raw
        FROM textos_pdf WHERE expediente = ? AND n_chars > 50
        ORDER BY tipo_documento
    ''', (expediente,)).fetchall()
    conn.close()
    return render_template_string(
        TEMPLATE_FICHA,
        css=CSS, l=l, lotes=lotes, criterios=criterios,
        documentos=documentos, textos_pliego=textos_pliego,
        tipo_label=tipo_label, active='buscar',
    )


@app.route('/doc/<int:doc_id>')
def ver_doc(doc_id):
    conn = get_db()
    doc = conn.execute('SELECT ruta_local, descargado FROM documentos WHERE id = ?', (doc_id,)).fetchone()
    conn.close()
    if not doc or not doc['descargado'] or not doc['ruta_local']:
        abort(404)
    if not os.path.exists(doc['ruta_local']):
        abort(404)
    return send_file(doc['ruta_local'], mimetype='application/pdf')


if __name__ == '__main__':
    print('Repositorio de Licitaciones arrancando...')
    print('Accede en: http://localhost:5000')
    app.run(debug=False, host='0.0.0.0', port=5000)
