import os
import json
import time
import re
import math
import shutil
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from openpyxl import load_workbook

from scoring import load_rules, total_score

# -----------------------------
# Config general
# -----------------------------
MP_TICKET = os.environ.get("MP_TICKET")

# GitHub repo para lectura de issues "reviewed"
GITHUB_REPOSITORY = os.environ.get("GITHUB_REPOSITORY", "danielquinterosr/periscopio-compras-publicas")

# En GitHub Actions el token estándar es GITHUB_TOKEN (si le diste permisos).
# En local, puedes exportarlo manualmente si quieres que "reviewed" funcione.
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")

# Paths salida
OUT_OPPS = "docs/data/opportunities.json"
OUT_META = "docs/data/meta.json"
OUT_REGISTRY = "docs/data/opportunities_registry.json"

# Compra Ágil: path del XLSX descargado automáticamente
COMPRA_AGIL_XLSX_PATH = os.environ.get("COMPRA_AGIL_XLSX_PATH", "data/compra_agil.xlsx")

# Archivo histórico diario (snapshot) del XLSX
HIST_DIR = Path("data/history/compra_agil")
HIST_DIR.mkdir(parents=True, exist_ok=True)

# Cache de detalle licitaciones (en disco)
CACHE_DIR = Path(".cache/mp_detail")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Regex IDs revisadas en issues: soporta licitaciones (1057489-550-LP25) y compra ágil (5178-6577-COT25)
ID_RE = re.compile(r"\b\d{3,7}-\d{1,6}-[A-Z0-9]{3,10}\b", re.UNICODE)

# -----------------------------
# HTTP session robusta
# -----------------------------
def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=0.8,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=30, pool_maxsize=30)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s

SESSION = make_session()

# -----------------------------
# Utils parsing
# -----------------------------
def safe_float(x):
    try:
        if x is None or x == "":
            return None
        if isinstance(x, str):
            x = x.replace(" ", "")
            if x.count(".") >= 1 and x.count(",") == 0:
                x = x.replace(".", "")
            if x.count(",") == 1 and x.count(".") == 0:
                x = x.replace(",", ".")
            if x.count(",") > 1:
                x = x.replace(",", "")
        return float(x)
    except Exception:
        return None

def parse_dt(s: Any) -> Optional[datetime]:
    """
    Parsea fechas típicas de MP y del Excel de Compra Ágil.
    """
    if s is None or s == "":
        return None

    # Si openpyxl ya entrega datetime
    if isinstance(s, datetime):
        return s

    s = str(s).strip()

    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass

    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

def iso_or_empty(dt: Optional[datetime]) -> str:
    if not dt:
        return ""
    # Mantengo string ISO sin forzar zona; el dashboard ya lo maneja como texto
    return dt.isoformat()

# -----------------------------
# Rules por fuente (defaults + by_source)
# -----------------------------
def deep_merge(a: dict, b: dict) -> dict:
    """
    merge b sobre a (b tiene precedencia).
    """
    out = dict(a or {})
    for k, v in (b or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = v
    return out

def rules_for_source(rules_all: dict, source: str) -> dict:
    defaults = rules_all.get("defaults") or {}
    by_source = rules_all.get("by_source") or {}
    src_rules = by_source.get(source) or {}
    return deep_merge(defaults, src_rules)

# -----------------------------
# Licitaciones (API MercadoPublico)
# -----------------------------
def cache_path_for_codigo(codigo: str) -> Path:
    safe = codigo.replace("/", "_")
    return CACHE_DIR / f"{safe}.json"

def fetch_licitaciones_activas(ticket: str) -> List[dict]:
    url = "https://api.mercadopublico.cl/servicios/v1/publico/licitaciones.json"
    r = SESSION.get(url, params={"estado": "activas", "ticket": ticket}, timeout=180)
    r.raise_for_status()
    data = r.json()
    return (
        data.get("Listado", [])
        or data.get("licitaciones", [])
        or data.get("ListadoLicitaciones", [])
        or []
    )

def fetch_licitacion_detalle(ticket: str, codigo_externo: str, use_cache: bool = True) -> dict:
    p = cache_path_for_codigo(codigo_externo)

    if use_cache and p.exists():
        try:
            with open(p, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass

    url = "https://api.mercadopublico.cl/servicios/v1/publico/licitaciones.json"
    r = SESSION.get(url, params={"codigo": codigo_externo, "ticket": ticket}, timeout=180)
    r.raise_for_status()
    data = r.json()

    if use_cache:
        try:
            with open(p, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    return data

def parse_detalle(det_json: dict) -> dict:
    listado = det_json.get("Listado") or det_json.get("licitaciones") or []
    item0 = listado[0] if isinstance(listado, list) and len(listado) > 0 else {}

    comprador = item0.get("Comprador") or {}
    fechas = item0.get("Fechas") or {}

    buyer = comprador.get("NombreOrganismo") or comprador.get("Nombre") or item0.get("NombreOrganismo") or ""
    published_at = fechas.get("FechaPublicacion") or item0.get("FechaPublicacion") or item0.get("FechaCreacion") or ""
    close_at = fechas.get("FechaCierre") or item0.get("FechaCierre") or item0.get("FechaCierreLicitacion") or ""
    questions_end_at = fechas.get("FechaFinal") or item0.get("FechaFinal") or item0.get("FechaFinPreguntas") or ""
    status = item0.get("Estado") or item0.get("EstadoLicitacion") or item0.get("estado") or ""

    amount_raw = (
        item0.get("MontoEstimado")
        or item0.get("Monto")
        or item0.get("PresupuestoEstimado")
        or item0.get("Presupuesto")
        or None
    )

    description = item0.get("Descripcion") or item0.get("DescripcionLicitacion") or ""

    return {
        "buyer": buyer,
        "published_at": published_at,
        "close_at": close_at,
        "questions_end_at": questions_end_at,
        "status": status,
        "amount_raw": amount_raw,
        "description": description,
    }

# -----------------------------
# Reviewed (GitHub Issues)
# -----------------------------
def fetch_reviewed_ids(repo: str, token: Optional[str]) -> set[str]:
    reviewed = set()
    if not repo:
        return reviewed

    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    def pull(params: dict):
        page = 1
        while True:
            url = f"https://api.github.com/repos/{repo}/issues"
            p = dict(params)
            p.update({"state": "all", "per_page": 100, "page": page})
            r = SESSION.get(url, headers=headers, params=p, timeout=60)
            if r.status_code >= 400:
                break

            items = r.json()
            if not items:
                break

            for it in items:
                if isinstance(it, dict) and it.get("pull_request"):
                    continue
                title = (it.get("title") or "").strip()
                m = ID_RE.search(title)
                if m:
                    reviewed.add(m.group(0))

            page += 1

    pull({"labels": "reviewed"})
    pull({})

    return reviewed

# -----------------------------
# Compra Ágil (Excel)
# -----------------------------
def archive_compra_agil_xlsx(src_path: str) -> Optional[str]:
    """
    Copia el XLSX a un histórico diario para no perder trazabilidad.
    Retorna path destino o None si no existía.
    """
    p = Path(src_path)
    if not p.exists():
        return None
    stamp = datetime.now(ZoneInfo("America/Santiago")).strftime("%Y%m%d")
    dst = HIST_DIR / f"compra_agil_{stamp}.xlsx"
    try:
        shutil.copyfile(p, dst)
        return str(dst)
    except Exception:
        return None

def load_compra_agil_rows(xlsx_path: str) -> List[dict]:
    """
    Lee el XLSX (primera hoja) y normaliza a filas dict con claves:
    id, title, published_at, close_at, buyer, unit, amount_clp, currency, status
    """
    p = Path(xlsx_path)
    if not p.exists():
        return []

    wb = load_workbook(filename=str(p), read_only=True, data_only=True)
    ws = wb.active

    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        return []

    headers = [str(h).strip() if h is not None else "" for h in rows[0]]
    data_rows = rows[1:]

    def idx(name: str) -> int:
        # match flexible por header
        name_l = name.lower()
        for i, h in enumerate(headers):
            if h and h.strip().lower() == name_l:
                return i
        return -1

    i_id = idx("ID")
    i_nombre = idx("Nombre")
    i_pub = idx("Fecha de Publicación")
    i_cierre = idx("Fecha de cierre")
    i_org = idx("Organismo")
    i_unidad = idx("Unidad")
    i_monto = idx("Monto Disponible")
    i_moneda = idx("Moneda")
    i_estado = idx("Estado")

    out = []
    for r in data_rows:
        if not r or all(v is None or v == "" for v in r):
            continue

        get = lambda j: r[j] if (j is not None and j >= 0 and j < len(r)) else None

        _id = (str(get(i_id)).strip() if get(i_id) is not None else "")
        if not _id:
            continue

        title = str(get(i_nombre) or "").strip()
        buyer = str(get(i_org) or "").strip()
        unit = str(get(i_unidad) or "").strip()
        currency = str(get(i_moneda) or "").strip()
        status = str(get(i_estado) or "").strip()

        pub_dt = parse_dt(get(i_pub))
        close_dt = parse_dt(get(i_cierre))

        amt = safe_float(get(i_monto))
        # En el XLSX vienen enteros sin separadores; safe_float lo deja OK.

        out.append({
            "id": _id,
            "title": title,
            "buyer": buyer,
            "unit": unit,
            "amount_clp": amt,
            "currency": currency,
            "status": status,
            "published_at": iso_or_empty(pub_dt),
            "close_at": iso_or_empty(close_dt),
        })

    return out

# -----------------------------
# Registry histórico
# -----------------------------
def load_registry(path: str) -> dict:
    p = Path(path)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}

def save_registry(path: str, reg: dict) -> None:
    os.makedirs(str(Path(path).parent), exist_ok=True)
    Path(path).write_text(json.dumps(reg, ensure_ascii=False, indent=2), encoding="utf-8")

def key_for(source: str, _id: str) -> str:
    return f"{source}:{_id}"

# -----------------------------
# Main
# -----------------------------
def main():
    if not MP_TICKET:
        raise RuntimeError("MP_TICKET no está definido (Secret/Env).")

    tz_cl = ZoneInfo("America/Santiago")
    now_cl = datetime.now(tz_cl)
    now_utc_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    rules_all = load_rules()

    # Parámetros licitaciones
    CANDIDATES_TOP = int(os.environ.get("CANDIDATES_TOP", "800"))
    MAX_DETAIL = int(os.environ.get("MAX_DETAIL", "400"))
    DETAIL_SLEEP = float(os.environ.get("DETAIL_SLEEP", "0.12"))

    # Revisadas (persistente vía issues)
    reviewed_ids = fetch_reviewed_ids(GITHUB_REPOSITORY, GITHUB_TOKEN)

    # -------------------------
    # 0) Registry previo
    # -------------------------
    registry = load_registry(OUT_REGISTRY)

    # -------------------------
    # 1) LICITACIONES
    # -------------------------
    rules_lic = rules_for_source(rules_all, "licitaciones")
    raw_list = fetch_licitaciones_activas(MP_TICKET)

    # Pre-score para priorizar detalle
    candidates: List[Tuple[int, dict]] = []
    for it in raw_list:
        codigo = it.get("CodigoExterno") or it.get("Codigo") or it.get("codigo")
        nombre = it.get("Nombre") or it.get("NombreLicitacion") or it.get("nombre") or (str(codigo) if codigo else "")
        buyer0 = it.get("NombreOrganismo") or ""
        pre_text = f"{nombre} {buyer0}"
        pre_score, _ = total_score(text=pre_text, amount_clp=None, rules=rules_lic)
        candidates.append((pre_score, it))

    candidates.sort(key=lambda x: x[0], reverse=True)
    detail_set = set()
    for _, it in candidates[:CANDIDATES_TOP]:
        codigo = it.get("CodigoExterno") or it.get("Codigo") or it.get("codigo")
        if codigo:
            detail_set.add(str(codigo))

    opps_current: List[dict] = []
    detail_ok = 0
    detail_fail = 0
    detail_count = 0

    for it in raw_list:
        codigo = it.get("CodigoExterno") or it.get("Codigo") or it.get("codigo")
        codigo = str(codigo) if codigo else ""
        if not codigo:
            continue

        nombre = it.get("Nombre") or it.get("NombreLicitacion") or it.get("nombre") or codigo

        # Defaults desde listado
        buyer = it.get("NombreOrganismo") or ""
        fecha_pub = it.get("FechaPublicacion") or it.get("FechaCreacion") or ""
        fecha_cierre = it.get("FechaCierre") or it.get("FechaCierreLicitacion") or ""
        monto = safe_float(it.get("MontoEstimado") or it.get("Monto") or it.get("monto"))
        descripcion = ""
        preguntas_hasta = it.get("FechaFinal") or ""
        status = it.get("Estado") or it.get("estado") or ""

        # Detalle solo para top y hasta MAX_DETAIL
        if codigo in detail_set and detail_count < MAX_DETAIL:
            detail_count += 1
            try:
                det_json = fetch_licitacion_detalle(MP_TICKET, codigo, use_cache=True)
                det = parse_detalle(det_json)

                buyer = det.get("buyer") or buyer
                fecha_pub = det.get("published_at") or fecha_pub
                fecha_cierre = det.get("close_at") or fecha_cierre
                preguntas_hasta = det.get("questions_end_at") or preguntas_hasta
                status = det.get("status") or status
                descripcion = det.get("description") or ""

                if det.get("amount_raw") is not None:
                    monto = safe_float(det.get("amount_raw"))

                detail_ok += 1
            except Exception:
                detail_fail += 1

            time.sleep(DETAIL_SLEEP)

        # Score final
        text_for_scoring = f"{nombre} {buyer} {descripcion}"
        score, score_detail = total_score(text=text_for_scoring, amount_clp=monto, rules=rules_lic)

        # Días al cierre licitación
        dias_cierre = None
        close_dt = parse_dt(fecha_cierre)
        if close_dt:
            if close_dt.tzinfo is None:
                close_dt = close_dt.replace(tzinfo=tz_cl)
            delta = close_dt - now_cl
            dias_cierre = max(0, int(math.ceil(delta.total_seconds() / 86400.0)))

        url = f"https://www.mercadopublico.cl/fichaLicitacion.html?idLicitacion={codigo}"

        reviewed = (codigo in reviewed_ids)

        opp = {
            "source": "licitaciones",
            "id": codigo,
            "title": nombre,
            "buyer": buyer,
            "status": status,
            "amount_clp": monto,
            "published_at": fecha_pub,
            "questions_end_at": preguntas_hasta,
            "close_at": fecha_cierre,
            "dias_cierre_licitacion": dias_cierre,
            "reviewed": reviewed,
            "score": score,
            "score_detail": score_detail,
            "url": url,
        }
        opps_current.append(opp)

        # Actualizar registry
        k = key_for("licitaciones", codigo)
        prev = registry.get(k, {})
        first_seen = prev.get("first_seen_at") or now_utc_iso
        times_seen = int(prev.get("times_seen") or 0) + 1
        registry[k] = {
            **prev,
            "source": "licitaciones",
            "id": codigo,
            "title": nombre,
            "buyer": buyer,
            "url": url,
            "first_seen_at": first_seen,
            "last_seen_at": now_utc_iso,
            "times_seen": times_seen,
            "reviewed": bool(prev.get("reviewed") or reviewed),
            "last_score": score,
        }

    # -------------------------
    # 2) COMPRA ÁGIL (Excel)
    # -------------------------
    rules_ca = rules_for_source(rules_all, "compra_agil")

    hist_path = archive_compra_agil_xlsx(COMPRA_AGIL_XLSX_PATH)
    compra_rows = load_compra_agil_rows(COMPRA_AGIL_XLSX_PATH)

    compra_opps = []
    for r in compra_rows:
        _id = r["id"]
        title = r["title"]
        buyer = r["buyer"]
        unit = r.get("unit") or ""
        status = r.get("status") or ""
        amount = r.get("amount_clp")
        published_at = r.get("published_at") or ""
        close_at = r.get("close_at") or ""
        url = f"https://buscador.mercadopublico.cl/compra-agil?palabraClave={_id}"  # fallback útil; no hay ficha tipo licitación

        # Para scoring: compra ágil no tiene descripción; usamos título + comprador + unidad
        text_for_scoring = f"{title} {buyer} {unit}"
        score, score_detail = total_score(text=text_for_scoring, amount_clp=amount, rules=rules_ca)

        reviewed = (_id in reviewed_ids)

        opp = {
            "source": "compra_agil",
            "id": _id,
            "title": title,
            "buyer": buyer,
            "status": status,
            "amount_clp": amount,
            "published_at": published_at,
            "close_at": close_at,
            "reviewed": reviewed,
            "score": score,
            "score_detail": score_detail,
            "url": url,
        }
        compra_opps.append(opp)

        # Actualizar registry
        k = key_for("compra_agil", _id)
        prev = registry.get(k, {})
        first_seen = prev.get("first_seen_at") or now_utc_iso
        times_seen = int(prev.get("times_seen") or 0) + 1
        registry[k] = {
            **prev,
            "source": "compra_agil",
            "id": _id,
            "title": title,
            "buyer": buyer,
            "url": url,
            "first_seen_at": first_seen,
            "last_seen_at": now_utc_iso,
            "times_seen": times_seen,
            "reviewed": bool(prev.get("reviewed") or reviewed),
            "last_score": score,
        }

    opps_current.extend(compra_opps)

    # -------------------------
    # 3) Filtrado “mostrar en dashboard”
    #    (umbral por fuente)
    # -------------------------
    show_min_lic = int((rules_lic.get("thresholds") or {}).get("show_min_score", 3))
    show_min_ca = int((rules_ca.get("thresholds") or {}).get("show_min_score", 3))

    opps_show = []
    for o in opps_current:
        src = o.get("source")
        s = o.get("score") or 0
        if src == "licitaciones" and s >= show_min_lic:
            opps_show.append(o)
        elif src == "compra_agil" and s >= show_min_ca:
            opps_show.append(o)

    opps_show.sort(key=lambda x: x.get("score") or 0, reverse=True)

    # Persistir registry
    save_registry(OUT_REGISTRY, registry)

    # Guardar outputs principales
    os.makedirs("docs/data", exist_ok=True)
    with open(OUT_OPPS, "w", encoding="utf-8") as f:
        json.dump(opps_show, f, ensure_ascii=False, indent=2)

    meta = {
        "last_update_iso": now_utc_iso,
        "repo": GITHUB_REPOSITORY,
        "paths": {
            "compra_agil_xlsx": COMPRA_AGIL_XLSX_PATH,
            "compra_agil_history_last": hist_path,
            "registry": OUT_REGISTRY,
        },
        "counts": {
            "total_current": len(opps_current),
            "shown": len(opps_show),
            "licitaciones_total": len([o for o in opps_current if o["source"] == "licitaciones"]),
            "compra_agil_total": len([o for o in opps_current if o["source"] == "compra_agil"]),
            "reviewed_ids_from_issues": len(reviewed_ids),
            "detalle_ok": detail_ok,
            "detalle_fail": detail_fail,
            "candidates_top": CANDIDATES_TOP,
            "max_detail": MAX_DETAIL,
        },
        "version": "v0.7",
    }

    with open(OUT_META, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
