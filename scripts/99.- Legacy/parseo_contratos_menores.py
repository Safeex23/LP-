# parseo_contratos_menores.py - Contratos Menores (contratosMenoresPerfilesContratantes.atom)
# Fuente: Contratos de menor cuantia del Sector Publico.

import lxml.etree as ET
import pandas as pd
import unicodedata
import os
import re
import requests

NS = {
    'atom':    'http://www.w3.org/2005/Atom',
    'cbc':     'urn:dgpe:names:draft:codice:schema:xsd:CommonBasicComponents-2',
    'cac':     'urn:dgpe:names:draft:codice:schema:xsd:CommonAggregateComponents-2',
    'cac_ext': 'urn:dgpe:names:draft:codice-place-ext:schema:xsd:CommonAggregateComponents-2',
    'cbc_ext': 'urn:dgpe:names:draft:codice-place-ext:schema:xsd:CommonBasicComponents-2',
}

FUENTE = 'Contratos Menores'

RUTA_ATOM  = r"C:\proyectos\licitaciones\datos\Menores\raw\2026\03\contratosMenoresPerfilesContratantes.atom"
RUTA_CSV   = r"C:\proyectos\licitaciones\datos\Menores\csv\2026\03\contratos_menores_final.csv"
RUTA_GEO   = r"C:\proyectos\licitaciones\datos\Menores\geo\municipios_ine.csv"

# ── MAPEOS (identicos a los otros dos scripts) ────────────────────
MAP_ESTADOS = {
    'PUB': 'Publicada', 'PRE': 'Anuncio Previo', 'RES': 'Resuelta',
    'ADJ': 'Adjudicada', 'ANUL': 'Anulada', 'EVL': 'Evaluacion',
    'EV': 'Evaluacion', 'CPRE': 'Creada', 'EVP': 'Evaluacion Previa',
    'PADJ': 'Parcialmente Adjudicada', 'ADJP': 'Adjudicacion Provisional',
    'PRES': 'Parcialmente Resuelta', 'DESI': 'Desistida', 'CERR': 'Cerrada'
}
MAP_TIPOS = {
    '1': 'Suministros', '2': 'Servicios', '3': 'Obras',
    '21': 'Administrativo especial', '40': 'Privado',
    '7': 'Gestion de Servicios Publicos', '8': 'Concesion de Servicios',
    '10': 'Concesion de Obras Publicas', '9': 'Concesion de Obras',
    '22': 'Concesion de Servicios', '50': 'Patrimonial'
}
MAP_PROCEDIMIENTO = {
    '1': 'Abierto', '2': 'Restringido', '3': 'Negociado con publicidad',
    '4': 'Negociado sin publicidad', '5': 'Dialogo competitivo',
    '6': 'Concurso de proyectos', '8': 'Asociacion para la innovacion',
    '9': 'Basado en Acuerdo Marco', '10': 'Basado en sistema dinamico',
    '11': 'Licitacion con negociacion', '13': 'Abierto simplificado',
    '100': 'Normas Internas', '999': 'Otros'
}
MAP_TRAMITACION  = {'1': 'Ordinaria', '2': 'Urgente', '3': 'Emergencia'}
MAP_FINANCIACION = {'NO-EU': 'Sin financiacion UE', 'EU': 'Con financiacion UE'}

MAP_NUTS_CCAA = {
    'ES111': 'Galicia', 'ES112': 'Galicia', 'ES113': 'Galicia', 'ES114': 'Galicia',
    'ES120': 'Asturias', 'ES130': 'Cantabria',
    'ES211': 'Pais Vasco', 'ES212': 'Pais Vasco', 'ES213': 'Pais Vasco',
    'ES220': 'Navarra', 'ES230': 'La Rioja',
    'ES241': 'Aragon', 'ES242': 'Aragon', 'ES243': 'Aragon',
    'ES300': 'Madrid',
    'ES411': 'Castilla y Leon', 'ES412': 'Castilla y Leon', 'ES413': 'Castilla y Leon',
    'ES414': 'Castilla y Leon', 'ES415': 'Castilla y Leon', 'ES416': 'Castilla y Leon',
    'ES417': 'Castilla y Leon', 'ES418': 'Castilla y Leon', 'ES419': 'Castilla y Leon',
    'ES421': 'Castilla-La Mancha', 'ES422': 'Castilla-La Mancha', 'ES423': 'Castilla-La Mancha',
    'ES424': 'Castilla-La Mancha', 'ES425': 'Castilla-La Mancha',
    'ES431': 'Extremadura', 'ES432': 'Extremadura',
    'ES511': 'Cataluna', 'ES512': 'Cataluna', 'ES513': 'Cataluna', 'ES514': 'Cataluna',
    'ES521': 'Comunidad Valenciana', 'ES522': 'Comunidad Valenciana', 'ES523': 'Comunidad Valenciana',
    'ES530': 'Baleares',
    'ES611': 'Andalucia', 'ES612': 'Andalucia', 'ES613': 'Andalucia', 'ES614': 'Andalucia',
    'ES615': 'Andalucia', 'ES616': 'Andalucia', 'ES617': 'Andalucia', 'ES618': 'Andalucia',
    'ES620': 'Murcia', 'ES630': 'Ceuta', 'ES640': 'Melilla',
    'ES701': 'Canarias', 'ES702': 'Canarias',
}
MAP_PROV = {
    '01': 'Alava', '02': 'Albacete', '03': 'Alicante', '04': 'Almeria',
    '05': 'Avila', '06': 'Badajoz', '07': 'Baleares', '08': 'Barcelona',
    '09': 'Burgos', '10': 'Caceres', '11': 'Cadiz', '12': 'Castellon',
    '13': 'Ciudad Real', '14': 'Cordoba', '15': 'La Coruna', '16': 'Cuenca',
    '17': 'Gerona', '18': 'Granada', '19': 'Guadalajara', '20': 'Guipuzcoa',
    '21': 'Huelva', '22': 'Huesca', '23': 'Jaen', '24': 'Leon',
    '25': 'Lerida', '26': 'La Rioja', '27': 'Lugo', '28': 'Madrid',
    '29': 'Malaga', '30': 'Murcia', '31': 'Navarra', '32': 'Orense',
    '33': 'Asturias', '34': 'Palencia', '35': 'Las Palmas', '36': 'Pontevedra',
    '37': 'Salamanca', '38': 'Santa Cruz de Tenerife', '39': 'Cantabria',
    '40': 'Segovia', '41': 'Sevilla', '42': 'Soria', '43': 'Tarragona',
    '44': 'Teruel', '45': 'Toledo', '46': 'Valencia', '47': 'Valladolid',
    '48': 'Vizcaya', '49': 'Zamora', '50': 'Zaragoza',
    '51': 'Ceuta', '52': 'Melilla',
}
MAP_PROV_CCAA = {
    '01': 'Pais Vasco', '02': 'Castilla-La Mancha', '03': 'Comunidad Valenciana',
    '04': 'Andalucia', '05': 'Castilla y Leon', '06': 'Extremadura',
    '07': 'Baleares', '08': 'Cataluna', '09': 'Castilla y Leon',
    '10': 'Extremadura', '11': 'Andalucia', '12': 'Comunidad Valenciana',
    '13': 'Castilla-La Mancha', '14': 'Andalucia', '15': 'Galicia',
    '16': 'Castilla-La Mancha', '17': 'Cataluna', '18': 'Andalucia',
    '19': 'Castilla-La Mancha', '20': 'Pais Vasco', '21': 'Andalucia',
    '22': 'Aragon', '23': 'Andalucia', '24': 'Castilla y Leon',
    '25': 'Cataluna', '26': 'La Rioja', '27': 'Galicia', '28': 'Madrid',
    '29': 'Andalucia', '30': 'Murcia', '31': 'Navarra', '32': 'Galicia',
    '33': 'Asturias', '34': 'Castilla y Leon', '35': 'Canarias',
    '36': 'Galicia', '37': 'Castilla y Leon', '38': 'Canarias',
    '39': 'Cantabria', '40': 'Castilla y Leon', '41': 'Andalucia',
    '42': 'Castilla y Leon', '43': 'Cataluna', '44': 'Aragon',
    '45': 'Castilla-La Mancha', '46': 'Comunidad Valenciana',
    '47': 'Castilla y Leon', '48': 'Pais Vasco', '49': 'Castilla y Leon',
    '50': 'Aragon', '51': 'Ceuta', '52': 'Melilla',
}
MAP_PRESENTACION = {
    '1': 'Electronica',
    '2': 'Manual',
    '3': 'Ambas'
}

_GEO_LOOKUP = None

def extraer_fecha_aviso(cfs, tipo_codigo):
    """Extrae la fecha del ValidNoticeInfo que coincide con el tipo de anuncio."""
    try:
        nodos = cfs.findall('.//cac_ext:ValidNoticeInfo', NS)
        for nodo in nodos:
            tipo = nodo.find('cbc_ext:NoticeTypeCode', NS)
            if tipo is not None and tipo.text == tipo_codigo:
                fecha = nodo.find('.//cac_ext:AdditionalPublicationDocumentReference/cbc:IssueDate', NS)
                if fecha is not None and fecha.text:
                    return limpiar(fecha.text)
        return 'Sin dato'
    except Exception:
        return 'Sin dato'

def descargar_geo_ine():
    os.makedirs(os.path.dirname(RUTA_GEO), exist_ok=True)
    if os.path.exists(RUTA_GEO):
        return
    print("  Descargando tabla municipios INE...")
    url = "https://raw.githubusercontent.com/codeforspain/ds-organizacion-administrativa/master/data/municipios.csv"
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        with open(RUTA_GEO, 'wb') as f:
            f.write(r.content)
        print("  Tabla INE descargada.")
    except Exception as e:
        print(f"  No se pudo descargar tabla INE: {e}")

def cargar_geo_ine():
    if not os.path.exists(RUTA_GEO):
        return {}
    try:
        df = pd.read_csv(RUTA_GEO, dtype=str, encoding='utf-8')
        lookup = {}
        for _, row in df.iterrows():
            nombre   = str(row.get('nombre', '')).strip()
            mun_id   = str(row.get('municipio_id', '')).strip().zfill(5)
            cod_prov = mun_id[:2]
            lookup[quitar_acentos(nombre).lower()] = (nombre, MAP_PROV.get(cod_prov, 'Sin dato'), MAP_PROV_CCAA.get(cod_prov, 'Sin dato'))
        print(f"  Tabla INE cargada: {len(lookup)} municipios.")
        return lookup
    except Exception as e:
        print(f"  Error cargando tabla INE: {e}")
        return {}

def get_geo_lookup():
    global _GEO_LOOKUP
    if _GEO_LOOKUP is None:
        descargar_geo_ine()
        _GEO_LOOKUP = cargar_geo_ine()
    return _GEO_LOOKUP

def quitar_acentos(texto):
    if not texto:
        return ''
    return ''.join(c for c in unicodedata.normalize('NFD', str(texto)) if unicodedata.category(c) != 'Mn')

def limpiar(texto):
    if not texto:
        return 'Sin dato'
    resultado = ' '.join(str(texto).split())
    return resultado if resultado else 'Sin dato'

def limpiar_ciudad(texto):
    if not texto or texto == 'Sin dato':
        return 'Sin dato'
    sin_par = re.sub(r'\s*\(.*?\)', '', texto)
    resultado = ' '.join(sin_par.split()).strip()
    return resultado.title() if resultado else 'Sin dato'

def extraer_texto(elemento, xpath, ns=NS):
    try:
        resultado = elemento.xpath(xpath, namespaces=ns)
        if resultado:
            r   = resultado[0]
            val = r.text if hasattr(r, 'text') else str(r)
            return limpiar(val)
        return 'Sin dato'
    except Exception:
        return 'Sin dato'

def extraer_todos_cpv(elemento, xpath, ns=NS):
    try:
        resultados = elemento.xpath(xpath, namespaces=ns)
        valores = []
        for r in resultados:
            raw  = r.text if hasattr(r, 'text') else str(r)
            solo = ''.join(filter(str.isdigit, raw or '')).zfill(8)
            if solo and solo != '00000000':
                valores.append(solo)
        return ' | '.join(valores) if valores else 'Sin dato'
    except Exception:
        return 'Sin dato'

def limpiar_importe(valor_texto):
    try:
        if not valor_texto or valor_texto == 'Sin dato':
            return 0.0
        return round(float(valor_texto.strip().replace(',', '.')), 2)
    except (ValueError, AttributeError):
        return 0.0

def resolver_geo(nuts_raw, ccaa_raw, ciudad_raw):
    lookup        = get_geo_lookup()
    ciudad_limpia = limpiar_ciudad(ciudad_raw)

    if ciudad_raw and ciudad_raw != 'Sin dato':
        ciudad_norm = quitar_acentos(limpiar_ciudad(ciudad_raw)).lower().strip()
        if ciudad_norm in lookup:
            _, provincia, ccaa = lookup[ciudad_norm]
            return ciudad_limpia, provincia, ccaa
        for clave, (_, prov, ca) in lookup.items():
            if ciudad_norm in clave or clave in ciudad_norm:
                return ciudad_limpia, prov, ca

    if nuts_raw and nuts_raw != 'Sin dato':
        ccaa = MAP_NUTS_CCAA.get(nuts_raw.strip().upper()[:5], 'Sin dato')
        if ccaa != 'Sin dato':
            return ciudad_limpia, 'Sin dato', ccaa

    if ccaa_raw and ccaa_raw != 'Sin dato' and len(ccaa_raw.strip()) > 3:
        return ciudad_limpia, 'Sin dato', ccaa_raw.strip().title()

    return ciudad_limpia, 'Sin dato', 'Sin dato'

def parsear_entry(entry):
    cfs = entry.find('.//cac_ext:ContractFolderStatus', NS)
    if cfs is None:
        return None

    est_c = extraer_texto(cfs, 'cbc_ext:ContractFolderStatusCode')
    tip_c = extraer_texto(cfs, './/cac:ProcurementProject/cbc:TypeCode')
    pro_c = extraer_texto(cfs, './/cac:TenderingProcess/cbc:ProcedureCode')
    tra_c = extraer_texto(cfs, './/cac:TenderingProcess/cbc:UrgencyCode')
    fin_c = extraer_texto(cfs, './/cac:TenderingTerms/cbc:FundingProgramCode')

    nuts_raw   = extraer_texto(cfs, './/cac:ProcurementProject/cac:RealizedLocation/cbc:CountrySubentityCode')
    ccaa_raw   = extraer_texto(cfs, './/cac:ProcurementProject/cac:RealizedLocation/cbc:CountrySubentity')
    ciudad_raw = extraer_texto(cfs, './/cac_ext:LocatedContractingParty/cac:Party/cac:PostalAddress/cbc:CityName')
    ciudad, provincia, ccaa = resolver_geo(nuts_raw, ccaa_raw, ciudad_raw)

    imp_est = limpiar_importe(extraer_texto(cfs, './/cac:ProcurementProject/cac:BudgetAmount/cbc:EstimatedOverallContractAmount'))
    imp_tot = limpiar_importe(extraer_texto(cfs, './/cac:ProcurementProject/cac:BudgetAmount/cbc:TotalAmount'))
    imp_sin = limpiar_importe(extraer_texto(cfs, './/cac:ProcurementProject/cac:BudgetAmount/cbc:TaxExclusiveAmount'))
    imp_efe = imp_tot if imp_tot > 0 else imp_est

    cpv_raw       = extraer_texto(cfs, './/cac:ProcurementProject/cac:RequiredCommodityClassification/cbc:ItemClassificationCode')
    cpv_principal = ''.join(filter(str.isdigit, cpv_raw)).zfill(8) if cpv_raw != 'Sin dato' else 'Sin dato'

    return {
        'fuente':                FUENTE,
        'expediente':            extraer_texto(cfs, 'cbc:ContractFolderID'),
        'estado':                MAP_ESTADOS.get(est_c, est_c),
        'enlace':                extraer_texto(entry, 'atom:link/@href'),
        'organo':                extraer_texto(cfs, './/cac_ext:LocatedContractingParty/cac:Party/cac:PartyName/cbc:Name'),
        'ciudad':                ciudad,
        'provincia':             provincia,
        'ccaa':                  ccaa,
        'nuts':                  nuts_raw,
        'pais':                  extraer_texto(cfs, './/cac_ext:LocatedContractingParty/cac:Party/cac:PostalAddress/cac:Country/cbc:Name'),
        'descripcion':           extraer_texto(cfs, './/cac:ProcurementProject/cbc:Name'),
        'tipo_contrato':         MAP_TIPOS.get(tip_c, tip_c),
        'subtipo':               extraer_texto(cfs, './/cac:ProcurementProject/cbc:SubTypeCode'),
        'cpv_principal':         cpv_principal,
        'cpv_todos':             extraer_todos_cpv(cfs, './/cac:ProcurementProject/cac:RequiredCommodityClassification/cbc:ItemClassificationCode'),
        'importe_efectivo':      imp_efe,
        'importe_total':         imp_tot,
        'importe_sin_iva':       imp_sin,
        'importe_estimado':      imp_est,
        'procedimiento':         MAP_PROCEDIMIENTO.get(pro_c, pro_c),
        'tramitacion':           MAP_TRAMITACION.get(tra_c, tra_c),
        'sistema_contrat':       extraer_texto(cfs, './/cac:TenderingProcess/cbc:ContractingSystemCode'),
        'forma_presentacion':    MAP_PRESENTACION.get(extraer_texto(cfs, './/cac:TenderingProcess/cbc:SubmissionMethodCode'), extraer_texto(cfs, './/cac:TenderingProcess/cbc:SubmissionMethodCode')),
        'sobre_umbral':          extraer_texto(cfs, './/cac:TenderingProcess/cbc:OverThresholdIndicator'),
        'financiacion_ue':       MAP_FINANCIACION.get(fin_c, fin_c),
        'fecha_publicacion':     extraer_texto(entry, 'atom:updated'),
        'fecha_limite':          extraer_texto(cfs, './/cac:TenderingProcess/cac:TenderSubmissionDeadlinePeriod/cbc:EndDate'),
        'hora_limite':           extraer_texto(cfs, './/cac:TenderingProcess/cac:TenderSubmissionDeadlinePeriod/cbc:EndTime'),
        'fecha_inicio_contrato': extraer_texto(cfs, './/cac:ProcurementProject/cac:PlannedPeriod/cbc:StartDate'),
        'fecha_fin_contrato':    extraer_texto(cfs, './/cac:ProcurementProject/cac:PlannedPeriod/cbc:EndDate'),
        'num_lotes':             len(cfs.findall('.//cac:ProcurementProjectLot', NS)),
        'url_pcap':              extraer_texto(cfs, './/cac:LegalDocumentReference/cac:Attachment/cac:ExternalReference/cbc:URI'),
        'url_ppt':               extraer_texto(cfs, './/cac:TechnicalDocumentReference/cac:Attachment/cac:ExternalReference/cbc:URI'),
        # ── CAMPOS DE ADJUDICACIÓN ────────────────────────────────────────
        'adjudicatario':          extraer_texto(cfs, './/cac:TenderResult/cac:WinningParty/cac:PartyName/cbc:Name'),
        'num_licitadores':        extraer_texto(cfs, './/cac:TenderResult/cbc:ReceivedTenderQuantity'),
        'importe_adjudicacion':   limpiar_importe(extraer_texto(cfs, './/cac:TenderResult/cac:AwardedTenderedProject/cac:LegalMonetaryTotal/cbc:TaxExclusiveAmount')),
        'fecha_contrato':         extraer_texto(cfs, './/cac:TenderResult/cac:Contract/cbc:IssueDate'),

        # ── FECHAS DE PUBLICACIÓN DE ANUNCIOS (ValidNoticeInfo) ───────────
        'fecha_anuncio_licitacion':  extraer_fecha_aviso(cfs, 'DOC_CN'),
        'fecha_anuncio_adjudicacion': extraer_fecha_aviso(cfs, 'DOC_CAN_ADJ'),
        'fecha_formalizacion':       extraer_fecha_aviso(cfs, 'DOC_FORM'),
        'fecha_modificacion':        extraer_fecha_aviso(cfs, 'DOC_MOD'),

    }


def parsear_atom(ruta_fichero):
    print(f"Parseando [{FUENTE}]: {ruta_fichero}")
    tree    = ET.parse(ruta_fichero)
    entries = tree.getroot().findall('atom:entry', NS)
    print(f"  Entradas encontradas: {len(entries)}")

    registros = []
    for e in entries:
        r = parsear_entry(e)
        if r:
            registros.append(r)

    df = pd.DataFrame(registros)

    for col in ['importe_efectivo', 'importe_total', 'importe_sin_iva', 'importe_estimado']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0).round(2)
    df['num_lotes'] = pd.to_numeric(df['num_lotes'], errors='coerce').fillna(0).astype(int)
    df['expediente'] = df['expediente'].astype(str)
    
    print(f"  Licitaciones procesadas: {len(df)} | Columnas: {len(df.columns)}")
    return df

if __name__ == "__main__":
    df = parsear_atom(RUTA_ATOM)

    print("\n── Primeras 5 licitaciones ──")
    print(df[['expediente', 'fuente', 'estado', 'ciudad', 'provincia', 'ccaa', 'importe_efectivo']].head(5).to_string())

    print("\n── CCAA mas frecuentes ──")
    print(df['ccaa'].value_counts().head(10).to_string())

    os.makedirs(os.path.dirname(RUTA_CSV), exist_ok=True)
    df.to_csv(RUTA_CSV, index=False, sep=';', encoding='utf-8-sig')
    print(f"\nCSV exportado: {RUTA_CSV}")
    print(f"Total: {len(df)} licitaciones | {len(df.columns)} columnas")