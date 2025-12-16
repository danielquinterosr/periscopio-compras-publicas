import os
import json
from datetime import datetime, timezone
import requests

from scoring import load_rules, total_score

MP_TICKET = os.environ.get("MP_TICKET")

OUT_OPPS = "docs/data/opportunities.json"
OUT_META = "docs/data/meta.json"

def fetch_licitaciones_activas(ticket: str) -> list[dict]:
    url = "https://api.mercadopublico.cl/servicios/v1/publico/licitaciones.json"
    r = requests.get(url, params={"estado": "activas", "ticket": ticket}, timeout=60)
    r.raise_for_status()
    data = r.json()
    # La API puede devolver distintas claves; tratamos variantes comunes
    return data.get("Listado", []) or data.get("licitaciones", []) or data.get("ListadoLicitaciones", []) or []

def safe_float(x):
    try:
        if x is None or x == "":
            return None
        # normaliza separadores comunes
        if isinstance(x, str):
            x = x.replace(".", "").replace(",", ".")
        return float(x)
    except Exception:
        return None

def main():
    if not MP_TICKET:
        raise RuntimeError("MP_TICKET no está definido. Configúralo como variable de entorno / GitHub Secret.")

    rules = load_rules()
    raw = fetch_licitaciones_activas(MP_TICKET)

    opps = []
    for it in raw:
        codigo = it.get("CodigoExterno") or it.get("Codigo") or it.get("codigo")
        nombre = it.get("Nombre") or it.get("NombreLicitacion") or it.get("nombre") or str(codigo)
        organismo = it.get("NombreOrganismo") or it.get("Comprador") or it.get("Organismo") or ""

        fecha_pub = it.get("FechaPublicacion") or it.get("FechaCreacion") or ""
        fecha_cierre = it.get("FechaCierre") or it.get("FechaCierreLicitacion") or ""

        # monto: muchas veces NO viene directo en el listado de activas; si no viene, quedará None (score_monto = 0)
        monto = safe_float(it.get("MontoEstimado") or it.get("Monto") or it.get("monto"))

        text = f"{nombre} {organismo} {json.dumps(it, ensure_ascii=False)}"
        score, detail = total_score(text=text, amount_clp=monto, rules=rules)

        url = ""
        if codigo:
            url = f"https://www.mercadopublico.cl/Procurement/Modules/RFB/DetailsAcquisition.aspx?qs={codigo}"

        opps.append({
            "source": "licitaciones",
            "id": str(codigo) if codigo else "",
            "title": nombre,
            "buyer": organismo,
            "published_at": fecha_pub,
            "close_at": fecha_cierre,
            "amount_clp": monto,
            "score": score,
            "score_detail": detail,
            "url": url
        })

    show_min = int((rules.get("thresholds") or {}).get("show_min_score", 3))
    opps_show = [o for o in opps if o["score"] >= show_min]
    opps_show.sort(key=lambda x: x["score"], reverse=True)

    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    meta = {
        "last_update_iso": now,
        "counts": {
            "licitaciones_total": len(opps),
            "licitaciones_mostradas": len(opps_show),
        },
        "version": "v0.1"
    }

    os.makedirs("docs/data", exist_ok=True)
    with open(OUT_OPPS, "w", encoding="utf-8") as f:
        json.dump(opps_show, f, ensure_ascii=False, indent=2)

    with open(OUT_META, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
