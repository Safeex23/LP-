#!/usr/bin/env python3
"""
parse_placsp.py — Parser completo de datos abiertos PLACSP (Atom/XML → CSV)

Extrae TODA la información de los ficheros .atom de la Plataforma de
Contratación del Sector Público y genera 4 ficheros CSV:

  1. licitaciones.csv — datos generales de cada expediente
  2. lotes.csv        — información desglosada por lote
  3. criterios_adjudicacion.csv — criterios de adjudicación (por lote o general)
  4. documentos.csv   — TODOS los documentos referenciados (pliegos, anuncios,
     adjudicaciones, informes, anexos, resoluciones, etc.) con URL de descarga

Uso:
    python parse_placsp.py --input ./atoms/ --output ./csv/
    python parse_placsp.py --input archivo.atom --output ./csv/ --fuente "Sector Publico" --periodo 202604

Requisitos:
    pip install lxml tqdm
"""

import argparse
import csv
import os
import sys
from datetime import datetime
from pathlib import Path
from lxml import etree
from tqdm import tqdm


# ---------------------------------------------------------------------------
# Namespaces CODICE / PLACSP
# Se detectan dinámicamente del fichero, pero estos son los más habituales.
# ---------------------------------------------------------------------------
DEFAULT_NS = {
    'atom':          'http://www.w3.org/2005/Atom',
    'cbc':           'urn:dgpe:names:draft:codice:schema:xsd:CommonBasicComponents-2',
    'cac':           'urn:dgpe:names:draft:codice:schema:xsd:CommonAggregateComponents-2',
    'cbc-place-ext': 'urn:dgpe:names:draft:codice-place-ext:schema:xsd:CommonBasicComponents-2',
    'cac-place-ext': 'urn:dgpe:names:draft:codice-place-ext:schema:xsd:CommonAggregateComponents-2',
    'at':            'http://purl.org/atompub/tombstones/1.0',
}

# ---------------------------------------------------------------------------
# Mapeos de códigos a texto legible
# ---------------------------------------------------------------------------
MAP_ESTADOS = {
    'PUB': 'Publicada', 'PRE': 'Anuncio Previo', 'RES': 'Resuelta',
    'ADJ': 'Adjudicada', 'ANUL': 'Anulada', 'EVL': 'Evaluacion',
    'EV': 'Evaluacion', 'CPRE': 'Creada', 'EVP': 'Evaluacion Previa',
    'PADJ': 'Parcialmente Adjudicada', 'ADJP': 'Adjudicacion Provisional',
    'PRES': 'Parcialmente Resuelta', 'DESI': 'Desistida', 'CERR': 'Cerrada',
}
MAP_PROCEDIMIENTO = {
    '1': 'Abierto', '2': 'Restringido', '3': 'Negociado con publicidad',
    '4': 'Negociado sin publicidad', '5': 'Dialogo competitivo',
    '6': 'Concurso de proyectos', '8': 'Asociacion para la innovacion',
    '9': 'Basado en Acuerdo Marco', '10': 'Basado en sistema dinamico',
    '11': 'Licitacion con negociacion', '13': 'Abierto simplificado',
    '100': 'Normas Internas', '999': 'Otros',
}
MAP_TRAMITACION = {'1': 'Ordinaria', '2': 'Urgente', '3': 'Emergencia'}
MAP_FINANCIACION = {'NO-EU': 'Sin financiacion UE', 'EU': 'Con financiacion UE'}

MAP_NUTS_CCAA = {
    'ES111': 'Galicia',            'ES112': 'Galicia',
    'ES113': 'Galicia',            'ES114': 'Galicia',
    'ES120': 'Asturias',           'ES130': 'Cantabria',
    'ES211': 'Pais Vasco',         'ES212': 'Pais Vasco',
    'ES213': 'Pais Vasco',         'ES220': 'Navarra',
    'ES230': 'La Rioja',
    'ES241': 'Aragon',             'ES242': 'Aragon',    'ES243': 'Aragon',
    'ES300': 'Madrid',
    'ES411': 'Castilla y Leon',    'ES412': 'Castilla y Leon',
    'ES413': 'Castilla y Leon',    'ES414': 'Castilla y Leon',
    'ES415': 'Castilla y Leon',    'ES416': 'Castilla y Leon',
    'ES417': 'Castilla y Leon',    'ES418': 'Castilla y Leon',
    'ES419': 'Castilla y Leon',
    'ES421': 'Castilla-La Mancha', 'ES422': 'Castilla-La Mancha',
    'ES423': 'Castilla-La Mancha', 'ES424': 'Castilla-La Mancha',
    'ES425': 'Castilla-La Mancha',
    'ES431': 'Extremadura',        'ES432': 'Extremadura',
    'ES511': 'Cataluna',           'ES512': 'Cataluna',
    'ES513': 'Cataluna',           'ES514': 'Cataluna',
    'ES521': 'Comunidad Valenciana', 'ES522': 'Comunidad Valenciana',
    'ES523': 'Comunidad Valenciana', 'ES530': 'Baleares',
    'ES611': 'Andalucia',          'ES612': 'Andalucia',
    'ES613': 'Andalucia',          'ES614': 'Andalucia',
    'ES615': 'Andalucia',          'ES616': 'Andalucia',
    'ES617': 'Andalucia',          'ES618': 'Andalucia',
    'ES620': 'Murcia',             'ES630': 'Ceuta',
    'ES640': 'Melilla',
    'ES701': 'Canarias',           'ES702': 'Canarias',
}


def _nuts_to_ccaa(nuts_raw):
    if not nuts_raw:
        return ''
    return MAP_NUTS_CCAA.get(nuts_raw.strip().upper()[:5], '')


# ---------------------------------------------------------------------------
# Utilidades de extracción XML
# ---------------------------------------------------------------------------

def txt(element, xpath, ns):
    """Extrae el texto de un elemento por XPath. Devuelve '' si no existe."""
    node = element.find(xpath, ns)
    if node is not None and node.text:
        return node.text.strip()
    return ''


def attr(element, xpath, attribute, ns):
    """Extrae un atributo de un elemento por XPath."""
    node = element.find(xpath, ns)
    if node is not None:
        return node.get(attribute, '')
    return ''


def txt_all(element, xpath, ns):
    """Extrae el texto de TODOS los nodos que coincidan con el XPath."""
    nodes = element.findall(xpath, ns)
    return [n.text.strip() for n in nodes if n is not None and n.text]


def detect_namespaces(root):
    """Detecta namespaces del elemento raíz del fichero atom."""
    ns = dict(DEFAULT_NS)
    for prefix, uri in root.nsmap.items():
        if prefix and uri:
            clean = prefix.replace(':', '-') if prefix else prefix
            ns[clean] = uri
    return ns


# ---------------------------------------------------------------------------
# Extracción de una entrada (licitación)
# ---------------------------------------------------------------------------

def parse_entry(entry, ns, fuente='', periodo=''):
    """
    Parsea un <entry> completo y devuelve:
      - licitacion: dict con todos los campos del expediente
      - lotes: lista de dicts (uno por lote)
      - criterios: lista de dicts (criterios de adjudicación)
      - documentos: lista de dicts (todos los documentos referenciados)
    """
    fecha_carga = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # --- Datos del entry Atom ---
    entry_id      = txt(entry, 'atom:id', ns)
    entry_link    = attr(entry, 'atom:link', 'href', ns)
    entry_title   = txt(entry, 'atom:title', ns)
    entry_summary = txt(entry, 'atom:summary', ns)
    entry_updated = txt(entry, 'atom:updated', ns)

    # --- Raíz de ContractFolderStatus ---
    cfs = entry.find('.//cac-place-ext:ContractFolderStatus', ns)
    if cfs is None:
        return None, [], [], []

    # --- Datos generales del expediente ---
    expediente = txt(cfs, 'cbc:ContractFolderID', ns)
    estado_code = txt(cfs, 'cbc-place-ext:ContractFolderStatusCode', ns)

    # --- Entidad adjudicadora ---
    lcp = cfs.find('cac-place-ext:LocatedContractingParty', ns)
    tipo_admin_code  = ''
    org_dir3         = ''
    org_nif          = ''
    org_nombre       = ''
    org_web          = ''
    org_ciudad       = ''
    org_cp           = ''
    org_provincia_code = ''
    org_provincia    = ''
    org_direccion    = ''
    org_pais         = ''
    buyer_profile    = ''

    if lcp is not None:
        tipo_admin_code = txt(lcp, 'cbc:ContractingPartyTypeCode', ns)
        buyer_profile   = txt(lcp, 'cbc-place-ext:BuyerProfileURIID', ns)

        party = lcp.find('cac:Party', ns)
        if party is not None:
            org_web = txt(party, 'cbc:WebsiteURI', ns)
            for pi in party.findall('cac:PartyIdentification', ns):
                scheme = attr(pi, 'cbc:ID', 'schemeName', ns)
                val    = txt(pi, 'cbc:ID', ns)
                if scheme == 'DIR3':
                    org_dir3 = val
                elif scheme == 'NIF':
                    org_nif = val
                elif not org_dir3:
                    org_dir3 = val

            org_nombre = txt(party, 'cac:PartyName/cbc:Name', ns)

            addr = party.find('cac:PostalAddress', ns)
            if addr is not None:
                org_ciudad         = txt(addr, 'cbc:CityName', ns)
                org_cp             = txt(addr, 'cbc:PostalZone', ns)
                org_provincia_code = txt(addr, 'cbc:CountrySubentityCode', ns)
                org_provincia      = txt(addr, 'cbc:CountrySubentity', ns)
                org_direccion      = txt(addr, 'cac:AddressLine/cbc:Line', ns)
                org_pais           = txt(addr, 'cac:Country/cbc:IdentificationCode', ns)

    # --- Procedimiento de licitación (TenderingProcess) ---
    tp = cfs.find('cac-place-ext:TenderingProcess', ns)
    procedimiento_code       = ''
    urgencia_code            = ''
    sistema_contratacion_code = ''
    metodo_presentacion      = ''
    fecha_limite_presentacion = ''
    hora_limite_presentacion  = ''
    descripcion_plazo         = ''
    fecha_disponibilidad_docs = ''

    if tp is not None:
        procedimiento_code        = txt(tp, 'cbc:ProcedureCode', ns)
        urgencia_code             = txt(tp, 'cbc:UrgencyCode', ns)
        sistema_contratacion_code = txt(tp, 'cbc-place-ext:ContractingSystemCode', ns)
        metodo_presentacion       = txt(tp, 'cbc:SubmissionMethodCode', ns)

        deadline = tp.find('cac:TenderSubmissionDeadlinePeriod', ns)
        if deadline is not None:
            fecha_limite_presentacion = txt(deadline, 'cbc:EndDate', ns)
            hora_limite_presentacion  = txt(deadline, 'cbc:EndTime', ns)
            descripcion_plazo         = txt(deadline, 'cbc:Description', ns)

        doc_avail = tp.find('cac:DocumentAvailabilityPeriod', ns)
        if doc_avail is not None:
            fecha_disponibilidad_docs = txt(doc_avail, 'cbc:EndDate', ns)

    # --- Condiciones de licitación (TenderingTerms) ---
    tt = cfs.find('cac-place-ext:TenderingTerms', ns)
    programa_financiacion_code = ''
    programa_financiacion      = ''
    idioma                     = ''
    condiciones_ejecucion      = ''
    subcontratacion_permitida  = ''
    garantia_tipo              = ''
    garantia_porcentaje        = ''
    garantia_descripcion       = ''
    solvencia_economica_desc   = ''
    solvencia_tecnica_desc     = ''

    if tt is not None:
        programa_financiacion_code = txt(tt, 'cbc:FundingProgramCode', ns)
        programa_financiacion      = txt(tt, 'cbc-place-ext:FundingProgram', ns)
        idioma                     = txt(tt, 'cac:Language/cbc:ID', ns)

        exec_terms = tt.find('cac-place-ext:ExecutionTerms', ns)
        if exec_terms is not None:
            condiciones_ejecucion = txt(exec_terms, 'cbc:Description', ns)

        grt = tt.find('cac:RequiredFinancialGuarantee', ns)
        if grt is not None:
            garantia_tipo       = txt(grt, 'cbc:GuaranteeTypeCode', ns)
            garantia_porcentaje = txt(grt, 'cbc:AmountRate', ns)
            garantia_descripcion = txt(grt, 'cbc:Description', ns)

        tqr = tt.find('cac-place-ext:TendererQualificationRequest', ns)
        if tqr is not None:
            for crit in tqr.findall('cac:FinancialEvaluationCriteria', ns):
                desc = txt(crit, 'cbc:Description', ns)
                if desc:
                    solvencia_economica_desc += desc + ' | '
            for crit in tqr.findall('cac:TechnicalEvaluationCriteria', ns):
                desc = txt(crit, 'cbc:Description', ns)
                if desc:
                    solvencia_tecnica_desc += desc + ' | '
            solvencia_economica_desc = solvencia_economica_desc.rstrip(' | ')
            solvencia_tecnica_desc   = solvencia_tecnica_desc.rstrip(' | ')

        sub = tt.find('cac:AllowedSubcontractTerms', ns)
        if sub is not None:
            subcontratacion_permitida = txt(sub, 'cbc:Description', ns)

    # --- Datos del contrato (ProcurementProject) ---
    pp_node = cfs.find('cac:ProcurementProject', ns)
    objeto_contrato              = ''
    tipo_contrato_code           = ''
    subtipo_contrato_code        = ''
    presupuesto_estimado         = ''
    importe_licitacion_sin_iva   = ''
    importe_licitacion_con_iva   = ''
    cpv_principal                = ''
    cpvs_adicionales             = ''
    lugar_ejecucion_prov         = ''
    lugar_ejecucion_prov_code    = ''
    lugar_ejecucion_pais         = ''
    lugar_ejecucion_nuts         = ''
    duracion_contrato            = ''
    duracion_unidad              = ''
    extension_opciones           = ''

    if pp_node is not None:
        objeto_contrato       = txt(pp_node, 'cbc:Name', ns)
        tipo_contrato_code    = txt(pp_node, 'cbc:TypeCode', ns)
        subtipo_contrato_code = txt(pp_node, 'cbc-place-ext:SubTypeCode', ns)

        budget = pp_node.find('cac:BudgetAmount', ns)
        if budget is not None:
            presupuesto_estimado       = txt(budget, 'cbc:EstimatedOverallContractAmount', ns)
            importe_licitacion_sin_iva = txt(budget, 'cbc:TotalAmount', ns)
            importe_licitacion_con_iva = txt(budget, 'cbc:TaxExclusiveAmount', ns)

        cpv_codes = []
        for rcc in pp_node.findall('cac:RequiredCommodityClassification', ns):
            code = txt(rcc, 'cbc:ItemClassificationCode', ns)
            if code:
                cpv_codes.append(code)
        if cpv_codes:
            cpv_principal    = cpv_codes[0]
            cpvs_adicionales = ';'.join(cpv_codes[1:]) if len(cpv_codes) > 1 else ''

        rl = pp_node.find('cac:RealizedLocation', ns)
        if rl is not None:
            lugar_ejecucion_prov      = txt(rl, 'cbc:CountrySubentity', ns)
            lugar_ejecucion_prov_code = txt(rl, 'cbc:CountrySubentityCode', ns)
            lugar_ejecucion_pais      = txt(rl, 'cac:Address/cac:Country/cbc:IdentificationCode', ns)
            lugar_ejecucion_nuts      = txt(rl, 'cbc:CountrySubentityCode', ns)

        planned = pp_node.find('cac:PlannedPeriod', ns)
        if planned is not None:
            dur_node = planned.find('cbc:DurationMeasure', ns)
            if dur_node is not None:
                duracion_contrato = dur_node.text.strip() if dur_node.text else ''
                duracion_unidad   = dur_node.get('unitCode', '')
            if not duracion_contrato:
                duracion_contrato = txt(planned, 'cbc:Description', ns)

        ext = pp_node.find('cac:ContractExtension', ns)
        if ext is not None:
            extension_opciones = txt(ext, 'cbc:OptionsDescription', ns)

    # --- Resultados de adjudicación (TenderResult) ---
    resultados_adj = []
    for tr in cfs.findall('cac-place-ext:TenderResult', ns):
        r = {}
        r['resultado_code']        = txt(tr, 'cbc:ResultCode', ns)
        r['resultado_descripcion'] = txt(tr, 'cbc:Description', ns)
        r['fecha_adjudicacion']    = txt(tr, 'cbc:AwardDate', ns)
        r['num_ofertas_recibidas'] = txt(tr, 'cbc:ReceivedTenderQuantity', ns)

        wp = tr.find('cac:WinningParty', ns)
        if wp is not None:
            r['adjudicatario_nif']    = ''
            for pi in wp.findall('cac:PartyIdentification', ns):
                r['adjudicatario_nif'] = txt(pi, 'cbc:ID', ns)
            r['adjudicatario_nombre'] = txt(wp, 'cac:PartyName/cbc:Name', ns)
        else:
            r['adjudicatario_nif']    = ''
            r['adjudicatario_nombre'] = ''

        atp = tr.find('cac:AwardedTenderedProject', ns)
        if atp is not None:
            r['lote_adjudicado']     = txt(atp, 'cac:ProcurementProjectLotID', ns)
            ba = atp.find('cac:BudgetAmount', ns)
            if ba is not None:
                r['importe_adjudicacion'] = txt(ba, 'cbc:TotalAmount', ns)
                r['importe_adj_sin_iva']  = txt(ba, 'cbc:TaxExclusiveAmount', ns)
            else:
                r['importe_adjudicacion'] = ''
                r['importe_adj_sin_iva']  = ''
        else:
            r['lote_adjudicado']      = ''
            r['importe_adjudicacion'] = ''
            r['importe_adj_sin_iva']  = ''

        r['fecha_inicio_contrato'] = txt(tr, 'cac-place-ext:StartDate', ns) or txt(tr, 'cbc:StartDate', ns)

        contract = tr.find('cac:Contract', ns)
        r['fecha_formalizacion'] = txt(contract, 'cbc:IssueDate', ns) if contract is not None else ''

        resultados_adj.append(r)

    adj = resultados_adj[0] if resultados_adj else {}

    # --- Criterios de adjudicación (nivel expediente) ---
    criterios = []
    if tt is not None:
        at_node = tt.find('cac:AwardingTerms', ns)
        if at_node is not None:
            for ac in at_node.findall('cac:AwardingCriteria', ns):
                criterios.append({
                    'expediente':    expediente,
                    'lote_id':       '',
                    'tipo_criterio': txt(ac, 'cbc:AwardingCriteriaTypeCode', ns),
                    'descripcion':   txt(ac, 'cbc:Description', ns),
                    'peso':          txt(ac, 'cbc:WeightNumeric', ns),
                })

    # --- Lotes (ProcurementProjectLot) ---
    lotes = []
    for ppl in cfs.findall('cac:ProcurementProjectLot', ns):
        lote = {}
        lote['expediente'] = expediente
        lote['lote_id']    = txt(ppl, 'cbc:ID', ns)

        lpp = ppl.find('cac:ProcurementProject', ns)
        if lpp is not None:
            lote['objeto_lote'] = txt(lpp, 'cbc:Name', ns)
            lb = lpp.find('cac:BudgetAmount', ns)
            lote['importe_lote'] = txt(lb, 'cbc:TotalAmount', ns) if lb is not None else ''
            lote_cpvs = []
            for rcc in lpp.findall('cac:RequiredCommodityClassification', ns):
                code = txt(rcc, 'cbc:ItemClassificationCode', ns)
                if code:
                    lote_cpvs.append(code)
            lote['cpv_lote'] = ';'.join(lote_cpvs)
        else:
            lote['objeto_lote']  = ''
            lote['importe_lote'] = ''
            lote['cpv_lote']     = ''

        ltt = ppl.find('cac-place-ext:TenderingTerms', ns)
        if ltt is not None:
            lat = ltt.find('cac:AwardingTerms', ns)
            if lat is not None:
                for ac in lat.findall('cac:AwardingCriteria', ns):
                    criterios.append({
                        'expediente':    expediente,
                        'lote_id':       lote['lote_id'],
                        'tipo_criterio': txt(ac, 'cbc:AwardingCriteriaTypeCode', ns),
                        'descripcion':   txt(ac, 'cbc:Description', ns),
                        'peso':          txt(ac, 'cbc:WeightNumeric', ns),
                    })

        for r in resultados_adj:
            if r.get('lote_adjudicado') == lote['lote_id']:
                lote['adjudicatario_nombre'] = r.get('adjudicatario_nombre', '')
                lote['importe_adjudicacion'] = r.get('importe_adjudicacion', '')
                break
        else:
            lote.setdefault('adjudicatario_nombre', '')
            lote.setdefault('importe_adjudicacion', '')

        lotes.append(lote)

    # --- DOCUMENTOS — captura TODOS los tipos ---
    documentos = []

    for doc in cfs.findall('.//cac:LegalDocumentReference', ns):
        documentos.append(_parse_doc_ref(doc, ns, expediente, 'pliego_administrativo'))

    for doc in cfs.findall('.//cac:TechnicalDocumentReference', ns):
        documentos.append(_parse_doc_ref(doc, ns, expediente, 'pliego_tecnico'))

    for doc in cfs.findall('.//cac:AdditionalDocumentReference', ns):
        documentos.append(_parse_doc_ref(doc, ns, expediente, 'documento_adicional'))

    for vni in cfs.findall('cac-place-ext:ValidNoticeInfo', ns):
        notice_type = txt(vni, 'cbc-place-ext:NoticeTypeCode', ns)
        for aps in vni.findall('cac-place-ext:AdditionalPublicationStatus', ns):
            medio = txt(aps, 'cbc-place-ext:PublicationMediaName', ns)
            for apdr in aps.findall('cac-place-ext:AdditionalPublicationDocumentReference', ns):
                fecha  = txt(apdr, 'cbc:IssueDate', ns)
                doc_id = txt(apdr, 'cbc:ID', ns)
                uri    = ''
                att    = apdr.find('cac:Attachment', ns)
                if att is not None:
                    er = att.find('cac:ExternalReference', ns)
                    if er is not None:
                        uri = txt(er, 'cbc:URI', ns)
                documentos.append({
                    'expediente':        expediente,
                    'tipo_documento':    f'anuncio_{notice_type}',
                    'doc_id':            doc_id,
                    'doc_url':           uri,
                    'doc_filename':      '',
                    'medio_publicacion': medio,
                    'fecha_publicacion': fecha,
                })

    # --- Construir la fila principal de la licitación ---
    licitacion = {
        'expediente':       expediente,
        'fuente':           fuente,
        'periodo':          periodo,
        'fecha_carga':      fecha_carga,
        'entry_id':         entry_id,
        'entry_link':       entry_link,
        'entry_updated':    entry_updated,

        # Estado (código + texto)
        'estado_code':      estado_code,
        'estado':           MAP_ESTADOS.get(estado_code, estado_code),

        # Objeto del contrato
        'titulo':           entry_title or objeto_contrato,
        'objeto_contrato':  objeto_contrato,
        'resumen':          entry_summary,

        # Órgano de contratación
        'tipo_admin_code':     tipo_admin_code,
        'org_dir3':            org_dir3,
        'org_nif':             org_nif,
        'org_nombre':          org_nombre,
        'org_web':             org_web,
        'org_ciudad':          org_ciudad,
        'org_cp':              org_cp,
        'org_provincia_code':  org_provincia_code,
        'org_provincia':       org_provincia,
        'org_direccion':       org_direccion,
        'org_pais':            org_pais,
        'buyer_profile':       buyer_profile,

        # Geografía (NUTS → CCAA)
        'lugar_ejecucion_prov':      lugar_ejecucion_prov,
        'lugar_ejecucion_prov_code': lugar_ejecucion_prov_code,
        'lugar_ejecucion_pais':      lugar_ejecucion_pais,
        'lugar_ejecucion_nuts':      lugar_ejecucion_nuts,
        'ccaa':                      _nuts_to_ccaa(lugar_ejecucion_nuts),

        # Procedimiento (código + texto)
        'procedimiento_code':        procedimiento_code,
        'procedimiento':             MAP_PROCEDIMIENTO.get(procedimiento_code, procedimiento_code),
        'urgencia_code':             urgencia_code,
        'tramitacion':               MAP_TRAMITACION.get(urgencia_code, urgencia_code),
        'sistema_contratacion_code': sistema_contratacion_code,
        'metodo_presentacion':       metodo_presentacion,
        'fecha_limite_presentacion': fecha_limite_presentacion,
        'hora_limite_presentacion':  hora_limite_presentacion,
        'descripcion_plazo':         descripcion_plazo,
        'fecha_disponibilidad_docs': fecha_disponibilidad_docs,

        # Contrato
        'tipo_contrato_code':        tipo_contrato_code,
        'subtipo_contrato_code':     subtipo_contrato_code,
        'presupuesto_estimado':      presupuesto_estimado,
        'importe_licitacion_sin_iva': importe_licitacion_sin_iva,
        'importe_licitacion_con_iva': importe_licitacion_con_iva,
        'cpv_principal':             cpv_principal,
        'cpvs_adicionales':          cpvs_adicionales,
        'duracion_contrato':         duracion_contrato,
        'duracion_unidad':           duracion_unidad,
        'extension_opciones':        extension_opciones,

        # Condiciones
        'programa_financiacion_code': programa_financiacion_code,
        'financiacion':               MAP_FINANCIACION.get(programa_financiacion_code, programa_financiacion_code),
        'programa_financiacion':      programa_financiacion,
        'idioma':                     idioma,
        'condiciones_ejecucion':      condiciones_ejecucion,
        'subcontratacion_permitida':  subcontratacion_permitida,
        'garantia_tipo':              garantia_tipo,
        'garantia_porcentaje':        garantia_porcentaje,
        'garantia_descripcion':       garantia_descripcion,
        'solvencia_economica':        solvencia_economica_desc,
        'solvencia_tecnica':          solvencia_tecnica_desc,

        # Resultado adjudicación
        'resultado_code':        adj.get('resultado_code', ''),
        'fecha_adjudicacion':    adj.get('fecha_adjudicacion', ''),
        'num_ofertas_recibidas': adj.get('num_ofertas_recibidas', ''),
        'adjudicatario_nif':     adj.get('adjudicatario_nif', ''),
        'adjudicatario_nombre':  adj.get('adjudicatario_nombre', ''),
        'importe_adjudicacion':  adj.get('importe_adjudicacion', ''),
        'importe_adj_sin_iva':   adj.get('importe_adj_sin_iva', ''),
        'fecha_formalizacion':   adj.get('fecha_formalizacion', ''),

        # Conteos
        'num_lotes':      str(len(lotes)),
        'num_documentos': str(len(documentos)),
    }

    return licitacion, lotes, criterios, documentos


def _parse_doc_ref(doc, ns, expediente, tipo):
    """Parsea un nodo DocumentReference (Legal, Technical, Additional)."""
    doc_id   = txt(doc, 'cbc:ID', ns)
    uri      = ''
    filename = ''

    att = doc.find('cac:Attachment', ns)
    if att is not None:
        er = att.find('cac:ExternalReference', ns)
        if er is not None:
            uri      = txt(er, 'cbc:URI', ns)
            filename = txt(er, 'cbc:FileName', ns)
            if not uri:
                uri = txt(er, 'cbc:DocumentHash', ns)

    return {
        'expediente':        expediente,
        'tipo_documento':    tipo,
        'doc_id':            doc_id,
        'doc_url':           uri,
        'doc_filename':      filename,
        'medio_publicacion': '',
        'fecha_publicacion': '',
    }


# ---------------------------------------------------------------------------
# Parseo de un fichero .atom completo
# ---------------------------------------------------------------------------

def parse_atom_file(filepath, fuente='', periodo='', ns=None):
    """
    Parsea un fichero .atom y devuelve listas de licitaciones, lotes,
    criterios y documentos.
    """
    licitaciones = []
    lotes        = []
    criterios    = []
    documentos   = []

    try:
        tree = etree.parse(str(filepath))
    except etree.XMLSyntaxError as e:
        print(f"  ERROR XML en {filepath}: {e}", file=sys.stderr)
        return licitaciones, lotes, criterios, documentos

    root = tree.getroot()

    if ns is None:
        ns = detect_namespaces(root)

    entries = root.findall('atom:entry', ns)
    if not entries:
        entries = root.findall('{http://www.w3.org/2005/Atom}entry')

    for entry in entries:
        try:
            lic, lots, crits, docs = parse_entry(entry, ns, fuente=fuente, periodo=periodo)
            if lic:
                licitaciones.append(lic)
                lotes.extend(lots)
                criterios.extend(crits)
                documentos.extend(docs)
        except Exception as e:
            entry_id_node = entry.find('{http://www.w3.org/2005/Atom}id')
            entry_id_text = entry_id_node.text if entry_id_node is not None else '??'
            print(f"  WARN: Error parseando entrada {entry_id_text}: {e}", file=sys.stderr)

    return licitaciones, lotes, criterios, documentos


# ---------------------------------------------------------------------------
# Escritura de CSVs
# ---------------------------------------------------------------------------

def write_csv(filepath, rows, fieldnames=None):
    """Escribe una lista de dicts a CSV (sep=;, utf-8-sig, comillas dobles)."""
    if not rows:
        print(f"  (sin datos para {filepath})")
        return

    if fieldnames is None:
        fieldnames = list(rows[0].keys())

    with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore',
                                delimiter=';', quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(rows)

    print(f"  -> {filepath} ({len(rows)} filas)")


# ---------------------------------------------------------------------------
# Main (uso directo desde línea de comandos)
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description='Parser completo de datos abiertos PLACSP (Atom/XML -> CSV)')
    parser.add_argument('--input', '-i', required=True,
                        help='Carpeta con ficheros .atom o un fichero .atom concreto')
    parser.add_argument('--output', '-o', default='./csv',
                        help='Carpeta de salida para los CSV (por defecto: ./csv)')
    parser.add_argument('--fuente', default='',
                        help='Fuente de datos (ej: "Sector Publico", "Agregacion", "Contratos Menores")')
    parser.add_argument('--periodo', default='',
                        help='Periodo en formato AAAAMM (ej: 202604)')
    args = parser.parse_args()

    input_path  = Path(args.input)
    output_path = Path(args.output)
    output_path.mkdir(parents=True, exist_ok=True)

    if input_path.is_file():
        atom_files = [input_path]
    elif input_path.is_dir():
        atom_files = sorted(input_path.glob('**/*.atom'))
    else:
        print(f"ERROR: {input_path} no existe.", file=sys.stderr)
        sys.exit(1)

    if not atom_files:
        print(f"ERROR: No se encontraron ficheros .atom en {input_path}", file=sys.stderr)
        sys.exit(1)

    print(f"Encontrados {len(atom_files)} ficheros .atom")

    all_licitaciones = []
    all_lotes        = []
    all_criterios    = []
    all_documentos   = []

    for af in tqdm(atom_files, desc="Parseando"):
        lics, lots, crits, docs = parse_atom_file(
            af, fuente=args.fuente, periodo=args.periodo
        )
        all_licitaciones.extend(lics)
        all_lotes.extend(lots)
        all_criterios.extend(crits)
        all_documentos.extend(docs)

    print(f"\nTotal: {len(all_licitaciones)} licitaciones, {len(all_lotes)} lotes, "
          f"{len(all_criterios)} criterios, {len(all_documentos)} documentos")

    # Deduplicar por expediente (quedarse con la entrada más reciente)
    seen = {}
    for lic in all_licitaciones:
        exp = lic['expediente']
        if exp not in seen or lic['entry_updated'] > seen[exp]['entry_updated']:
            seen[exp] = lic
    all_licitaciones = list(seen.values())
    print(f"Tras deduplicar: {len(all_licitaciones)} licitaciones unicas")

    write_csv(output_path / 'licitaciones.csv',          all_licitaciones)
    write_csv(output_path / 'lotes.csv',                 all_lotes)
    write_csv(output_path / 'criterios_adjudicacion.csv', all_criterios)
    write_csv(output_path / 'documentos.csv',            all_documentos)

    doc_types = {}
    for d in all_documentos:
        t = d['tipo_documento']
        doc_types[t] = doc_types.get(t, 0) + 1
    print("\nDocumentos por tipo:")
    for t, count in sorted(doc_types.items(), key=lambda x: -x[1]):
        print(f"  {t}: {count}")

    docs_con_url = [d for d in all_documentos if d['doc_url']]
    print(f"\nDocumentos con URL descargable: {len(docs_con_url)} de {len(all_documentos)}")


if __name__ == '__main__':
    main()
