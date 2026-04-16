"""
app_salud.py — Flask: buscador de licitaciones del sector salud (CPV 33*/85*)
==============================================================================
Puerto 5001 (paralelo a app.py en 5000).

Rutas:
    /                    Buscador principal con filtros
    /licitacion/<exp>    Ficha completa con lotes, criterios, documentos
    /doc/<id>            Redirect a URL del documento (o sirve el PDF local)
    /pdf/<path>          Sirve PDFs descargados
    /api/stats           JSON con estadísticas de la BD
"""

import os
import json
import sqlite3
from flask import Flask, render_template_string, request, redirect, send_file, jsonify, abort

RUTA_DB   = r'C:\proyectos\licitaciones\salud\datos\licitaciones_salud.db'
RUTA_PDFS = r'C:\proyectos\licitaciones\salud\datos\pdfs'

app = Flask(__name__)


# ---------------------------------------------------------------------------
# BD helper
# ---------------------------------------------------------------------------
def get_db():
    conn = sqlite3.connect(RUTA_DB, timeout=30)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# Templates
# ---------------------------------------------------------------------------
BASE_HTML = """
<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{% block title %}Licitaciones Salud{% endblock %}</title>
  <style>
    * { box-sizing: border-box; }
    body { font-family: 'Segoe UI', Arial, sans-serif; margin: 0; background: #f5f7fa; color: #222; }
    nav { background: #1a6b4a; color: #fff; padding: 10px 24px; display:flex; align-items:center; gap:20px; }
    nav a { color:#b8efd4; text-decoration:none; font-weight:500; }
    nav a:hover { color:#fff; }
    nav .title { font-size:1.1em; font-weight:700; color:#fff; margin-right:auto; }
    .container { max-width: 1400px; margin: 0 auto; padding: 24px 16px; }
    .card { background:#fff; border-radius:8px; padding:20px; margin-bottom:16px; box-shadow:0 1px 4px rgba(0,0,0,.08); }
    h1 { font-size:1.4em; margin:0 0 16px; }
    h2 { font-size:1.1em; margin:16px 0 8px; color:#1a6b4a; }
    .filters { display:flex; flex-wrap:wrap; gap:10px; align-items:flex-end; }
    .filters label { font-size:.82em; color:#555; display:block; margin-bottom:3px; }
    .filters input, .filters select { padding:6px 10px; border:1px solid #ccc; border-radius:5px; font-size:.9em; }
    .filters button { padding:7px 20px; background:#1a6b4a; color:#fff; border:none; border-radius:5px; cursor:pointer; font-size:.9em; }
    .filters button:hover { background:#145a3d; }
    table { width:100%; border-collapse:collapse; font-size:.88em; }
    th { background:#f0f4f1; text-align:left; padding:8px 10px; border-bottom:2px solid #dde; color:#444; }
    td { padding:7px 10px; border-bottom:1px solid #eee; vertical-align:top; }
    tr:hover td { background:#f8fffe; }
    .badge { display:inline-block; padding:2px 8px; border-radius:10px; font-size:.78em; font-weight:600; }
    .badge-pub { background:#dbeafe; color:#1d4ed8; }
    .badge-res { background:#dcfce7; color:#166534; }
    .badge-adj { background:#bbf7d0; color:#14532d; }
    .badge-eval { background:#fef9c3; color:#854d0e; }
    .badge-anul { background:#fee2e2; color:#991b1b; }
    .badge-pre  { background:#e0e7ff; color:#3730a3; }
    .importe { text-align:right; font-variant-numeric:tabular-nums; }
    a { color:#1a6b4a; }
    a:hover { color:#0f4a2e; }
    .meta { font-size:.82em; color:#666; }
    .pill { display:inline-block; background:#e8f5ef; color:#1a6b4a; border-radius:12px; padding:2px 10px; font-size:.8em; margin:2px; }
    .section-title { font-size:.95em; font-weight:700; color:#1a6b4a; border-bottom:1px solid #d0e8db; padding-bottom:5px; margin:18px 0 10px; }
    .stats-grid { display:grid; grid-template-columns:repeat(auto-fit, minmax(160px,1fr)); gap:12px; margin-bottom:16px; }
    .stat-box { background:#f0f9f4; border:1px solid #c6e8d5; border-radius:8px; padding:14px; text-align:center; }
    .stat-num { font-size:1.8em; font-weight:700; color:#1a6b4a; }
    .stat-lbl { font-size:.8em; color:#666; margin-top:4px; }
    .doc-row { display:flex; align-items:center; gap:8px; padding:5px 0; border-bottom:1px solid #f0f0f0; font-size:.85em; }
    .doc-row:last-child { border-bottom:none; }
    .doc-tipo { min-width:200px; color:#555; }
    .doc-link { color:#1a6b4a; }
    .no-url { color:#aaa; font-style:italic; font-size:.82em; }
    .pagination { display:flex; gap:8px; align-items:center; margin-top:12px; }
    .pagination a, .pagination span { padding:5px 12px; border:1px solid #ccc; border-radius:4px; font-size:.85em; }
    .pagination a { color:#1a6b4a; text-decoration:none; }
    .pagination span { background:#1a6b4a; color:#fff; border-color:#1a6b4a; }
    .back-link { display:inline-block; margin-bottom:12px; font-size:.88em; }
  </style>
</head>
<body>
<nav>
  <span class="title">🏥 Licitaciones Salud</span>
  <a href="/">Buscar</a>
  <a href="/api/stats">Stats JSON</a>
</nav>
<div class="container">
{% block content %}{% endblock %}
</div>
</body>
</html>
"""

BUSCADOR_HTML = BASE_HTML.replace('{% block content %}{% endblock %}', """
{% block content %}
<div class="card">
  <h1>Buscador de licitaciones sanitarias</h1>
  <form method="get" action="/">
    <div class="filters">
      <div>
        <label>Texto (objeto / órgano)</label>
        <input name="q" value="{{ q }}" placeholder="Ej: equipos médicos..." style="width:280px">
      </div>
      <div>
        <label>CCAA</label>
        <select name="ccaa">
          <option value="">Todas</option>
          {% for c in ccaas %}<option value="{{ c }}" {% if c==ccaa_sel %}selected{% endif %}>{{ c }}</option>{% endfor %}
        </select>
      </div>
      <div>
        <label>Estado</label>
        <select name="estado">
          <option value="">Todos</option>
          {% for e in estados %}<option value="{{ e }}" {% if e==estado_sel %}selected{% endif %}>{{ e }}</option>{% endfor %}
        </select>
      </div>
      <div>
        <label>CPV</label>
        <input name="cpv" value="{{ cpv_sel }}" placeholder="Ej: 33600000" style="width:130px">
      </div>
      <div>
        <label>Importe mín (€)</label>
        <input name="imp_min" value="{{ imp_min }}" placeholder="0" style="width:110px" type="number">
      </div>
      <div>
        <label>Importe máx (€)</label>
        <input name="imp_max" value="{{ imp_max }}" placeholder="sin límite" style="width:110px" type="number">
      </div>
      <div>
        <label>Fuente</label>
        <select name="fuente">
          <option value="">Todas</option>
          {% for f in fuentes %}<option value="{{ f }}" {% if f==fuente_sel %}selected{% endif %}>{{ f }}</option>{% endfor %}
        </select>
      </div>
      <div style="align-self:flex-end">
        <button type="submit">Buscar</button>
      </div>
    </div>
  </form>
</div>

<div class="card">
  <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;">
    <span style="font-size:.9em; color:#555;">{{ total }} resultado(s) {% if q %}para "<b>{{ q }}</b>"{% endif %}</span>
    <span style="font-size:.82em; color:#999;">Página {{ page }}/{{ total_pages }}</span>
  </div>
  <table>
    <thead>
      <tr>
        <th>Expediente</th>
        <th>Objeto</th>
        <th>Órgano contratante</th>
        <th>CCAA</th>
        <th>CPV</th>
        <th>Importe s/IVA</th>
        <th>Estado</th>
        <th>F. Límite</th>
      </tr>
    </thead>
    <tbody>
    {% for r in rows %}
    <tr>
      <td><a href="/licitacion/{{ r.expediente }}">{{ r.expediente }}</a></td>
      <td>{{ (r.objeto_contrato or r.titulo or '')[:80] }}{% if (r.objeto_contrato or r.titulo or '')|length > 80 %}…{% endif %}</td>
      <td class="meta">{{ (r.org_nombre or '')[:50] }}{% if (r.org_nombre or '')|length > 50 %}…{% endif %}</td>
      <td><span class="pill">{{ r.ccaa or '—' }}</span></td>
      <td class="meta">{{ r.cpv_principal or '—' }}</td>
      <td class="importe">{{ '{:,.0f}'.format(r.importe_licitacion_sin_iva) if r.importe_licitacion_sin_iva else '—' }}</td>
      <td>
        {% set estado = r.estado or '' %}
        {% if 'Publicada' in estado %}<span class="badge badge-pub">{{ estado }}</span>
        {% elif 'Resuelta' in estado %}<span class="badge badge-res">{{ estado }}</span>
        {% elif 'Adjudicada' in estado %}<span class="badge badge-adj">{{ estado }}</span>
        {% elif 'Evaluacion' in estado %}<span class="badge badge-eval">{{ estado }}</span>
        {% elif 'Anulada' in estado or 'Desistida' in estado %}<span class="badge badge-anul">{{ estado }}</span>
        {% elif 'Previo' in estado %}<span class="badge badge-pre">{{ estado }}</span>
        {% else %}<span class="badge">{{ estado }}</span>{% endif %}
      </td>
      <td class="meta">{{ r.fecha_limite_presentacion or '—' }}</td>
    </tr>
    {% else %}
    <tr><td colspan="8" style="text-align:center; color:#999; padding:30px;">Sin resultados</td></tr>
    {% endfor %}
    </tbody>
  </table>

  <div class="pagination">
    {% if page > 1 %}<a href="?{{ qs }}&page={{ page-1 }}">&laquo; Anterior</a>{% endif %}
    <span>{{ page }}</span>
    {% if page < total_pages %}<a href="?{{ qs }}&page={{ page+1 }}">Siguiente &raquo;</a>{% endif %}
  </div>
</div>
{% endblock %}
""")

FICHA_HTML = BASE_HTML.replace('{% block content %}{% endblock %}', """
{% block content %}
<a class="back-link" href="javascript:history.back()">&#8592; Volver</a>

<div class="card">
  <h1>{{ lic.objeto_contrato or lic.titulo or lic.expediente }}</h1>
  <div style="display:flex; gap:8px; flex-wrap:wrap; margin-bottom:12px;">
    {% set estado = lic.estado or '' %}
    {% if 'Publicada' in estado %}<span class="badge badge-pub">{{ estado }}</span>
    {% elif 'Resuelta' in estado %}<span class="badge badge-res">{{ estado }}</span>
    {% elif 'Adjudicada' in estado %}<span class="badge badge-adj">{{ estado }}</span>
    {% elif 'Evaluacion' in estado %}<span class="badge badge-eval">{{ estado }}</span>
    {% elif 'Anulada' in estado or 'Desistida' in estado %}<span class="badge badge-anul">{{ estado }}</span>
    {% elif 'Previo' in estado %}<span class="badge badge-pre">{{ estado }}</span>
    {% else %}<span class="badge">{{ estado }}</span>{% endif %}
    <span class="pill">{{ lic.cpv_principal or '' }}</span>
    <span class="pill">{{ lic.fuente or '' }}</span>
    {% if lic.ccaa %}<span class="pill">{{ lic.ccaa }}</span>{% endif %}
  </div>

  <div style="display:grid; grid-template-columns:repeat(auto-fit,minmax(280px,1fr)); gap:12px;">
    <div>
      <div class="section-title">Datos generales</div>
      <table style="font-size:.86em;">
        <tr><td style="color:#666;padding:3px 8px 3px 0">Expediente</td><td><b>{{ lic.expediente }}</b></td></tr>
        <tr><td style="color:#666;padding:3px 8px 3px 0">Tipo contrato</td><td>{{ lic.tipo_contrato_code or '—' }}</td></tr>
        <tr><td style="color:#666;padding:3px 8px 3px 0">Procedimiento</td><td>{{ lic.procedimiento or '—' }}</td></tr>
        <tr><td style="color:#666;padding:3px 8px 3px 0">Tramitación</td><td>{{ lic.tramitacion or '—' }}</td></tr>
        <tr><td style="color:#666;padding:3px 8px 3px 0">Duración</td><td>{{ lic.duracion_contrato or '—' }} {{ lic.duracion_unidad or '' }}</td></tr>
        <tr><td style="color:#666;padding:3px 8px 3px 0">Financiación</td><td>{{ lic.financiacion or '—' }}</td></tr>
        <tr><td style="color:#666;padding:3px 8px 3px 0">CPVs adicionales</td><td style="font-size:.82em">{{ lic.cpvs_adicionales or '—' }}</td></tr>
      </table>
    </div>
    <div>
      <div class="section-title">Importes</div>
      <table style="font-size:.86em;">
        <tr><td style="color:#666;padding:3px 8px 3px 0">Presupuesto estimado</td><td class="importe">{{ '{:,.0f} €'.format(lic.presupuesto_estimado) if lic.presupuesto_estimado else '—' }}</td></tr>
        <tr><td style="color:#666;padding:3px 8px 3px 0">Licitación s/IVA</td><td class="importe">{{ '{:,.0f} €'.format(lic.importe_licitacion_sin_iva) if lic.importe_licitacion_sin_iva else '—' }}</td></tr>
        <tr><td style="color:#666;padding:3px 8px 3px 0">Licitación c/IVA</td><td class="importe">{{ '{:,.0f} €'.format(lic.importe_licitacion_con_iva) if lic.importe_licitacion_con_iva else '—' }}</td></tr>
        <tr><td style="color:#666;padding:3px 8px 3px 0">Adjudicación</td><td class="importe">{{ '{:,.0f} €'.format(lic.importe_adjudicacion) if lic.importe_adjudicacion else '—' }}</td></tr>
        <tr><td style="color:#666;padding:3px 8px 3px 0">Adjudicatario</td><td>{{ lic.adjudicatario_nombre or '—' }}</td></tr>
        <tr><td style="color:#666;padding:3px 8px 3px 0">NIF adjudicatario</td><td>{{ lic.adjudicatario_nif or '—' }}</td></tr>
        <tr><td style="color:#666;padding:3px 8px 3px 0">Nº ofertas</td><td>{{ lic.num_ofertas_recibidas or '—' }}</td></tr>
      </table>
    </div>
    <div>
      <div class="section-title">Órgano contratante</div>
      <table style="font-size:.86em;">
        <tr><td style="color:#666;padding:3px 8px 3px 0">Nombre</td><td>{{ lic.org_nombre or '—' }}</td></tr>
        <tr><td style="color:#666;padding:3px 8px 3px 0">NIF</td><td>{{ lic.org_nif or '—' }}</td></tr>
        <tr><td style="color:#666;padding:3px 8px 3px 0">DIR3</td><td>{{ lic.org_dir3 or '—' }}</td></tr>
        <tr><td style="color:#666;padding:3px 8px 3px 0">Ciudad</td><td>{{ lic.org_ciudad or '—' }}</td></tr>
        <tr><td style="color:#666;padding:3px 8px 3px 0">CCAA</td><td>{{ lic.ccaa or '—' }}</td></tr>
        <tr><td style="color:#666;padding:3px 8px 3px 0">NUTS</td><td>{{ lic.lugar_ejecucion_nuts or '—' }}</td></tr>
        {% if lic.buyer_profile %}<tr><td style="color:#666;padding:3px 8px 3px 0">Perfil</td><td><a href="{{ lic.buyer_profile }}" target="_blank" style="font-size:.82em">Ver perfil ↗</a></td></tr>{% endif %}
        {% if lic.entry_link %}<tr><td style="color:#666;padding:3px 8px 3px 0">PLACSP</td><td><a href="{{ lic.entry_link }}" target="_blank" style="font-size:.82em">Ver ficha ↗</a></td></tr>{% endif %}
      </table>
    </div>
    <div>
      <div class="section-title">Plazos</div>
      <table style="font-size:.86em;">
        <tr><td style="color:#666;padding:3px 8px 3px 0">F. límite presentación</td><td>{{ lic.fecha_limite_presentacion or '—' }}</td></tr>
        <tr><td style="color:#666;padding:3px 8px 3px 0">Hora límite</td><td>{{ lic.hora_limite_presentacion or '—' }}</td></tr>
        <tr><td style="color:#666;padding:3px 8px 3px 0">F. adjudicación</td><td>{{ lic.fecha_adjudicacion or '—' }}</td></tr>
        <tr><td style="color:#666;padding:3px 8px 3px 0">F. formalización</td><td>{{ lic.fecha_formalizacion or '—' }}</td></tr>
      </table>
    </div>
  </div>

  {% if lic.solvencia_economica or lic.solvencia_tecnica %}
  <div class="section-title" style="margin-top:16px">Solvencia</div>
  <div style="font-size:.86em; display:grid; grid-template-columns:1fr 1fr; gap:12px;">
    <div><b>Económica:</b> {{ lic.solvencia_economica or '—' }}</div>
    <div><b>Técnica:</b>   {{ lic.solvencia_tecnica or '—' }}</div>
  </div>
  {% endif %}
</div>

{% if lotes %}
<div class="card">
  <div class="section-title" style="margin:0 0 10px">Lotes ({{ lotes|length }})</div>
  <table>
    <thead><tr><th>Lote</th><th>Objeto</th><th>CPV</th><th>Importe</th><th>Adjudicatario</th></tr></thead>
    <tbody>
    {% for l in lotes %}
    <tr>
      <td>{{ l.lote_id or '—' }}</td>
      <td>{{ l.objeto_lote or '—' }}</td>
      <td class="meta">{{ l.cpv_lote or '—' }}</td>
      <td class="importe">{{ '{:,.0f} €'.format(l.importe_lote) if l.importe_lote else '—' }}</td>
      <td>{{ l.adjudicatario_nombre or '—' }}</td>
    </tr>
    {% endfor %}
    </tbody>
  </table>
</div>
{% endif %}

{% if criterios %}
<div class="card">
  <div class="section-title" style="margin:0 0 10px">Criterios de adjudicación ({{ criterios|length }})</div>
  <table>
    <thead><tr><th>Tipo</th><th>Descripción</th><th>Peso</th></tr></thead>
    <tbody>
    {% for c in criterios %}
    <tr>
      <td class="meta">{{ c.tipo_criterio or '—' }}</td>
      <td>{{ c.descripcion or '—' }}</td>
      <td>{{ c.peso or '—' }}</td>
    </tr>
    {% endfor %}
    </tbody>
  </table>
</div>
{% endif %}

<div class="card">
  <div class="section-title" style="margin:0 0 10px">Documentos ({{ docs|length }})</div>
  {% if docs %}
  {% for d in docs %}
  <div class="doc-row">
    <span class="doc-tipo">{{ d.tipo_documento }}</span>
    {% if d.ruta_local %}
      <a class="doc-link" href="/pdf/{{ d.id }}" target="_blank">📄 {{ d.doc_filename or 'Abrir PDF' }}</a>
      <span style="font-size:.78em; color:#888;">(local)</span>
    {% elif d.doc_url %}
      <a class="doc-link" href="/doc/{{ d.id }}" target="_blank">🔗 {{ d.doc_filename or 'Ver documento' }}</a>
    {% else %}
      <span class="no-url">Sin URL directa — requiere Selenium</span>
    {% endif %}
    {% if d.fecha_publicacion %}<span class="meta" style="margin-left:auto">{{ d.fecha_publicacion }}</span>{% endif %}
  </div>
  {% endfor %}
  {% else %}
  <p style="color:#999; font-size:.88em">No hay documentos registrados.</p>
  {% endif %}
</div>
{% endblock %}
""")


# ---------------------------------------------------------------------------
# Rutas
# ---------------------------------------------------------------------------
@app.route('/')
def index():
    conn = get_db()

    # Filtros disponibles
    ccaas   = [r[0] for r in conn.execute("SELECT DISTINCT ccaa FROM licitaciones WHERE ccaa!='' ORDER BY ccaa").fetchall()]
    estados = [r[0] for r in conn.execute("SELECT DISTINCT estado FROM licitaciones WHERE estado!='' ORDER BY estado").fetchall()]
    fuentes = [r[0] for r in conn.execute("SELECT DISTINCT fuente FROM licitaciones WHERE fuente!='' ORDER BY fuente").fetchall()]

    q         = request.args.get('q', '').strip()
    ccaa_sel  = request.args.get('ccaa', '').strip()
    estado_sel= request.args.get('estado', '').strip()
    cpv_sel   = request.args.get('cpv', '').strip()
    imp_min   = request.args.get('imp_min', '').strip()
    imp_max   = request.args.get('imp_max', '').strip()
    fuente_sel= request.args.get('fuente', '').strip()
    page      = max(1, int(request.args.get('page', 1) or 1))
    per_page  = 50

    where = ['1=1']
    params = []

    if q:
        where.append('(objeto_contrato LIKE ? OR titulo LIKE ? OR org_nombre LIKE ? OR expediente LIKE ?)')
        params += [f'%{q}%'] * 4
    if ccaa_sel:
        where.append('ccaa = ?'); params.append(ccaa_sel)
    if estado_sel:
        where.append('estado = ?'); params.append(estado_sel)
    if cpv_sel:
        where.append('cpv_principal LIKE ?'); params.append(f'{cpv_sel}%')
    if imp_min:
        where.append('importe_licitacion_sin_iva >= ?'); params.append(float(imp_min))
    if imp_max:
        where.append('importe_licitacion_sin_iva <= ?'); params.append(float(imp_max))
    if fuente_sel:
        where.append('fuente = ?'); params.append(fuente_sel)

    sql_where = ' AND '.join(where)
    total = conn.execute(f'SELECT COUNT(*) FROM licitaciones WHERE {sql_where}', params).fetchone()[0]
    total_pages = max(1, (total + per_page - 1) // per_page)
    offset = (page - 1) * per_page

    rows = conn.execute(
        f'''SELECT expediente, titulo, objeto_contrato, org_nombre, ccaa, cpv_principal,
                   importe_licitacion_sin_iva, estado, fecha_limite_presentacion
            FROM licitaciones WHERE {sql_where}
            ORDER BY fecha_limite_presentacion DESC, expediente DESC
            LIMIT ? OFFSET ?''',
        params + [per_page, offset],
    ).fetchall()
    conn.close()

    # Construir query string para paginación
    qs_parts = []
    for k in ('q','ccaa','estado','cpv','imp_min','imp_max','fuente'):
        v = request.args.get(k,'')
        if v:
            qs_parts.append(f'{k}={v}')
    qs = '&'.join(qs_parts)

    return render_template_string(BUSCADOR_HTML,
        rows=rows, total=total, page=page, total_pages=total_pages, qs=qs,
        q=q, ccaa_sel=ccaa_sel, estado_sel=estado_sel, cpv_sel=cpv_sel,
        imp_min=imp_min, imp_max=imp_max, fuente_sel=fuente_sel,
        ccaas=ccaas, estados=estados, fuentes=fuentes,
    )


@app.route('/licitacion/<expediente>')
def ficha(expediente):
    conn = get_db()
    lic = conn.execute('SELECT * FROM licitaciones WHERE expediente=?', (expediente,)).fetchone()
    if not lic:
        conn.close()
        abort(404)
    lotes    = conn.execute('SELECT * FROM lotes    WHERE expediente=? ORDER BY lote_id', (expediente,)).fetchall()
    criterios= conn.execute('SELECT * FROM criterios WHERE expediente=? ORDER BY id', (expediente,)).fetchall()
    docs     = conn.execute('SELECT * FROM documentos WHERE expediente=? ORDER BY tipo_documento, fecha_publicacion DESC', (expediente,)).fetchall()
    conn.close()
    return render_template_string(FICHA_HTML, lic=lic, lotes=lotes, criterios=criterios, docs=docs)


@app.route('/doc/<int:doc_id>')
def redir_doc(doc_id):
    conn = get_db()
    doc = conn.execute('SELECT doc_url, ruta_local FROM documentos WHERE id=?', (doc_id,)).fetchone()
    conn.close()
    if not doc:
        abort(404)
    if doc['ruta_local'] and os.path.exists(doc['ruta_local']):
        return send_file(doc['ruta_local'], mimetype='application/pdf')
    if doc['doc_url']:
        return redirect(doc['doc_url'])
    abort(404)


@app.route('/pdf/<int:doc_id>')
def serve_pdf(doc_id):
    conn = get_db()
    doc = conn.execute('SELECT ruta_local, doc_filename FROM documentos WHERE id=?', (doc_id,)).fetchone()
    conn.close()
    if not doc or not doc['ruta_local'] or not os.path.exists(doc['ruta_local']):
        abort(404)
    return send_file(doc['ruta_local'], mimetype='application/pdf',
                     download_name=doc['doc_filename'] or 'documento.pdf')


@app.route('/api/stats')
def api_stats():
    conn = get_db()
    total      = conn.execute('SELECT COUNT(*) FROM licitaciones').fetchone()[0]
    tot_docs   = conn.execute('SELECT COUNT(*) FROM documentos').fetchone()[0]
    descargados= conn.execute('SELECT COUNT(*) FROM documentos WHERE descargado=1').fetchone()[0]
    con_url    = conn.execute("SELECT COUNT(*) FROM documentos WHERE doc_url!='' AND doc_url IS NOT NULL").fetchone()[0]
    importe    = conn.execute('SELECT SUM(importe_licitacion_sin_iva) FROM licitaciones WHERE importe_licitacion_sin_iva>0').fetchone()[0]

    por_estado = dict(conn.execute("SELECT estado, COUNT(*) FROM licitaciones GROUP BY estado ORDER BY 2 DESC").fetchall())
    por_ccaa   = dict(conn.execute("SELECT ccaa, COUNT(*) FROM licitaciones GROUP BY ccaa ORDER BY 2 DESC LIMIT 15").fetchall())
    por_cpv    = dict(conn.execute("SELECT cpv_principal, COUNT(*) FROM licitaciones GROUP BY cpv_principal ORDER BY 2 DESC LIMIT 15").fetchall())
    conn.close()

    return jsonify({
        'licitaciones': total,
        'documentos': {'total': tot_docs, 'con_url': con_url, 'descargados': descargados},
        'importe_total_eur': round(importe or 0, 2),
        'por_estado': por_estado,
        'por_ccaa': por_ccaa,
        'por_cpv': por_cpv,
    })


if __name__ == '__main__':
    print('Iniciando app salud en http://localhost:5001')
    print(f'BD: {RUTA_DB}')
    app.run(debug=True, port=5001, use_reloader=False)
