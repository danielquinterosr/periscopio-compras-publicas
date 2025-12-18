import os
import json
import time
import re
import math
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from scoring import load_rules, total_score

MP_TICKET = os.environ.get("MP_TICKET")

# GitHub repo para lectura de issues "reviewed"
GITHUB_REPOSITORY = os.environ.get(
    "GITHUB_REPOSITORY", "danielquinterosr/periscopio-compras-publicas"
)

# OJO: en GitHub Actions el token estándar es GITHUB_TOKEN (si le diste permisos).
# En local, puedes exportarlo manualmente si quieres que "reviewed" funcione.
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")

OUT_OPPS = "docs/data/opportunities.json"
OUT_META = "docs/data/meta.json"

# Cache de detalle por código (en disco)
CACHE_DIR = Path(".cache/mp_detail")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Regex para capturar un CódigoExterno típico (p.ej. 1057489-550-LP25)
CODE_RE = re.compile(r"\b\d{3,7}-\d{1,4}-[A-Z]{1,3}\d{1,4}\b")



def make_session() -> requests.Session:
    """
    Session con reintentos (429/5xx/timeouts) para robustez.
    """
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


def safe_float(x):
    try:
        if x is None or x == "":
            return None
        if isinstance(x, str):
            x = x.replace(" ", "")
            # si viene con separador miles "." y sin coma, lo quitamos
            if x.count(".") >= 1 and x.count(",") == 0:
                x = x.replace(".", "")
            # si viene con coma decimal y sin puntos, normalizamos a "."
            if x.count(",") == 1 and x.count(".") == 0:
                x = x.replace(",", ".")
            # si viene con muchas comas, las quitamos
            if x.count(",") > 1:
                x = x.replace(",", "")
        return float(x)
    except Exception:
        return None


def parse_dt(s: str):
    """
    Parsea fechas típicas de MP:
    - '2025-12-10T08:32:00'
    - '2025-12-10 08:32:00'
    - ISO con zona (a veces)
    """
    if not s:
        return None
    s = str(s).strip()

    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass

    # intento ISO general (por si viniera con offset o 'Z')
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def cache_path_for_codigo(codigo: str) -> Path:
    safe = codigo.replace("/", "_")
    return CACHE_DIR / f"{safe}.json"


def fetch_licitaciones_activas(ticket: str) -> list[dict]:
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
    """
    Detalle por CódigoExterno (codigo=...).
    Cachea la respuesta para no re-consultar siempre.
    """
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
    """
    Normaliza buyer, fechas, monto, descripción, estado y cierre de preguntas desde el JSON de detalle.
    """
    listado = det_json.get("Listado") or det_json.get("licitaciones") or []
    item0 = listado[0] if isinstance(listado, list) and len(listado) > 0 else {}

    comprador = item0.get("Comprador") or {}
    fechas = item0.get("Fechas") or {}

    buyer = (
        comprador.get("NombreOrganismo")
        or comprador.get("Nombre")
        or item0.get("NombreOrganismo")
        or ""
    )

    published_at = (
        fechas.get("FechaPublicacion")
        or item0.get("FechaPublicacion")
        or item0.get("FechaCreacion")
        or ""
    )

    # Cierre licitación (mantengo tu lógica)
    close_at = (
        fechas.get("FechaCierre")
        or item0.get("FechaCierre")
        or item0.get("FechaCierreLicitacion")
        or ""
    )

    # Cierre preguntas (según tu corrección: FechaFinal)
    questions_end_at = (
        fechas.get("FechaFinal")
        or item0.get("FechaFinal")
        or item0.get("FechaFinPreguntas")
        or ""
    )

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


def fetch_reviewed_ids(repo: str, token: str | None) -> set[str]:
    """
    Extrae IDs revisadas desde issues GitHub.

    Regla:
    - Siempre busca IDs por título (Reviewed: <ID>) como fuente principal.
    - Si existe label=reviewed, lo usa como filtro adicional (más limpio),
      pero NO dependemos de eso para funcionar.

    Esto permite:
    - Funcionar sin token en repos públicos.
    - Funcionar aunque el issue no tenga label.
    """
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
                m = CODE_RE.search(title)
                if m:
                    reviewed.add(m.group(0))

            page += 1

    # 1) intentamos por label (si existe)
    pull({"labels": "reviewed"})
    # 2) fallback sin label (clave para tu caso)
    pull({})

    return reviewed



def main():
    if not MP_TICKET:
        raise RuntimeError("MP_TICKET no está definido (Secret/Env).")

    rules = load_rules()

    # Parámetros opción B
    CANDIDATES_TOP = int(os.environ.get("CANDIDATES_TOP", "800"))
    MAX_DETAIL = int(os.environ.get("MAX_DETAIL", "400"))
    DETAIL_SLEEP = float(os.environ.get("DETAIL_SLEEP", "0.12"))

    # Revisadas persistentes (equipo)
    reviewed_ids = fetch_reviewed_ids(GITHUB_REPOSITORY, GITHUB_TOKEN)

    raw_list = fetch_licitaciones_activas(MP_TICKET)

    # 1) Score preliminar para seleccionar candidatos a detalle
    candidates = []
    for it in raw_list:
        codigo = it.get("CodigoExterno") or it.get("Codigo") or it.get("codigo")
        nombre = it.get("Nombre") or it.get("NombreLicitacion") or it.get("nombre") or (
            str(codigo) if codigo else ""
        )
        buyer0 = it.get("NombreOrganismo") or ""
        pre_text = f"{nombre} {buyer0}"
        pre_score, _ = total_score(text=pre_text, amount_clp=None, rules=rules)
        candidates.append((pre_score, it))

    candidates.sort(key=lambda x: x[0], reverse=True)

    detail_set = set()
    for _, it in candidates[:CANDIDATES_TOP]:
        codigo = it.get("CodigoExterno") or it.get("Codigo") or it.get("codigo")
        if codigo:
            detail_set.add(str(codigo))

    tz_cl = ZoneInfo("America/Santiago")
    now_cl = datetime.now(tz_cl)

    opps = []
    detail_ok = 0
    detail_fail = 0
    detail_count = 0

    for it in raw_list:
        codigo = it.get("CodigoExterno") or it.get("Codigo") or it.get("codigo")
        codigo = str(codigo) if codigo else ""

        nombre = it.get("Nombre") or it.get("NombreLicitacion") or it.get("nombre") or (codigo if codigo else "")

        # Defaults desde listado (por si no pedimos detalle)
        buyer = it.get("NombreOrganismo") or ""
        fecha_pub = it.get("FechaPublicacion") or it.get("FechaCreacion") or ""
        fecha_cierre = it.get("FechaCierre") or it.get("FechaCierreLicitacion") or ""
        monto = safe_float(it.get("MontoEstimado") or it.get("Monto") or it.get("monto"))
        descripcion = ""
        preguntas_hasta = it.get("FechaFinal") or ""
        status = it.get("Estado") or it.get("estado") or ""

        # Detalle solo para candidatos top y hasta MAX_DETAIL
        if codigo and (codigo in detail_set) and (detail_count < MAX_DETAIL):
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

        # Score final (con lo mejor disponible)
        text_for_scoring = f"{nombre} {buyer} {descripcion}"
        score, score_detail = total_score(text=text_for_scoring, amount_clp=monto, rules=rules)

        # Días al cierre licitación (Chile)
        dias_cierre = None
        close_dt = parse_dt(fecha_cierre)
        if close_dt:
            if close_dt.tzinfo is None:
                close_dt = close_dt.replace(tzinfo=tz_cl)
            delta = close_dt - now_cl
            dias_cierre = max(0, int(math.ceil(delta.total_seconds() / 86400.0)))

        url = f"https://www.mercadopublico.cl/fichaLicitacion.html?idLicitacion={codigo}" if codigo else ""

        opps.append({
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
            "reviewed": (codigo in reviewed_ids),
            "score": score,
            "score_detail": score_detail,
            "url": url
        })

    # Filtrar lo que se muestra
    show_min = int((rules.get("thresholds") or {}).get("show_min_score", 3))
    opps_show = [o for o in opps if (o.get("score") is not None and o["score"] >= show_min)]
    opps_show.sort(key=lambda x: x["score"], reverse=True)

    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    meta = {
        "last_update_iso": now,
        "repo": GITHUB_REPOSITORY,
        "counts": {
            "licitaciones_total": len(opps),
            "licitaciones_mostradas": len(opps_show),
            "detalle_ok": detail_ok,
            "detalle_fail": detail_fail,
            "candidates_top": CANDIDATES_TOP,
            "max_detail": MAX_DETAIL,
            "reviewed_ids": len(reviewed_ids),
        },
        "version": "v0.5"
    }

    os.makedirs("docs/data", exist_ok=True)
    with open(OUT_OPPS, "w", encoding="utf-8") as f:
        json.dump(opps_show, f, ensure_ascii=False, indent=2)

    with open(OUT_META, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
