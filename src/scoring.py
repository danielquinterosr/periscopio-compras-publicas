import re
import yaml
from typing import Any, Dict, List, Tuple, Optional


def load_rules(path: str = "config/rules.yml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _compile_patterns(items: List[dict]) -> List[dict]:
    """
    Compila patrones regex de include/exclude definidos como:
      - pattern: "encuest(a|as)"
        weight: 3
        note: "..."
    """
    compiled = []
    for it in items or []:
        pat = (it.get("pattern") or "").strip()
        if not pat:
            continue
        flags = re.IGNORECASE | re.UNICODE
        compiled.append({
            "pattern": pat,
            "weight": float(it.get("weight", 1)),
            "note": it.get("note", ""),
            "rx": re.compile(pat, flags),
        })
    return compiled


def _score_keywords(text: str, rules: dict) -> Tuple[float, Dict[str, Any]]:
    """
    Retorna score_keywords en escala 0..10 y detalle.
    Lógica:
      - suma pesos de matches include
      - resta pesos de matches exclude
      - aplica umbral mínimo (si hay excludes fuertes, puede quedar 0)
      - normaliza a 0..10 usando 'max_points' configurable
    """
    kw = (rules.get("keywords") or {})
    include = _compile_patterns(kw.get("include") or [])
    exclude = _compile_patterns(kw.get("exclude") or [])

    t = (text or "").strip()
    inc_hits = []
    exc_hits = []

    inc_points = 0.0
    exc_points = 0.0

    for it in include:
        if it["rx"].search(t):
            inc_points += it["weight"]
            inc_hits.append({"pattern": it["pattern"], "weight": it["weight"], "note": it["note"]})

    for it in exclude:
        if it["rx"].search(t):
            exc_points += it["weight"]
            exc_hits.append({"pattern": it["pattern"], "weight": it["weight"], "note": it["note"]})

    raw = inc_points - exc_points

    # Normalización: max_points define cuántos puntos equivalen a 10
    max_points = float(kw.get("max_points", 12))  # ajustable con calibración
    if max_points <= 0:
        max_points = 12.0

    score_0_10 = max(0.0, min(10.0, (raw / max_points) * 10.0))

    detail = {
        "keywords": {
            "include_points": inc_points,
            "exclude_points": exc_points,
            "raw_points": raw,
            "max_points": max_points,
            "score_0_10": score_0_10,
            "include_hits": inc_hits,
            "exclude_hits": exc_hits,
        }
    }
    return score_0_10, detail


def _amount_band_points(amount_clp: Optional[float], rules: dict) -> Tuple[float, Dict[str, Any]]:
    """
    Asigna puntos por tramo de monto según lo que pediste (+1..+7) y
    lo transforma a escala 0..10 para ponderación.
    """
    bands = (rules.get("amount_bands") or [])
    # bands: lista ordenada con {min, max, points, label}
    if amount_clp is None:
        return 0.0, {"amount": {"amount_clp": None, "band": None, "band_points": 0, "score_0_10": 0.0}}

    amt = float(amount_clp)

    chosen = None
    for b in bands:
        bmin = b.get("min", None)
        bmax = b.get("max", None)
        if bmin is None:
            bmin = float("-inf")
        if bmax is None:
            bmax = float("inf")
        if amt >= float(bmin) and amt < float(bmax):
            chosen = b
            break

    if not chosen and bands:
        chosen = bands[-1]

    band_points = float(chosen.get("points", 0)) if chosen else 0.0
    max_band_points = float(rules.get("amount_max_points", 7))  # por defecto 7

    score_0_10 = 0.0
    if max_band_points > 0:
        score_0_10 = max(0.0, min(10.0, (band_points / max_band_points) * 10.0))

    detail = {
        "amount": {
            "amount_clp": amt,
            "band": chosen.get("label") if chosen else None,
            "band_points": band_points,
            "score_0_10": score_0_10
        }
    }
    return score_0_10, detail


def total_score(text: str, amount_clp: Optional[float], rules: dict) -> Tuple[int, Dict[str, Any]]:
    """
    Scoring ponderado:
      score_total_0_10 = w_kw*score_kw + w_amt*score_amt
    Luego se convierte a una escala entera para el dashboard (0..20 por defecto).
    """

    weights = (rules.get("weights") or {})
    w_kw = float(weights.get("keywords", 0.7))
    w_amt = float(weights.get("amount", 0.3))

    # normalizar pesos si vienen mal
    s = w_kw + w_amt
    if s <= 0:
        w_kw, w_amt = 0.7, 0.3
        s = 1.0
    w_kw /= s
    w_amt /= s

    score_kw_0_10, det_kw = _score_keywords(text, rules)
    score_amt_0_10, det_amt = _amount_band_points(amount_clp, rules)

    score_total_0_10 = (w_kw * score_kw_0_10) + (w_amt * score_amt_0_10)

    # Escala final para mostrar (entero)
    display_max = int((rules.get("thresholds") or {}).get("display_max_score", 20))
    if display_max <= 0:
        display_max = 20

    score_display = int(round((score_total_0_10 / 10.0) * display_max))
    score_display = max(0, min(display_max, score_display))

    detail = {
        "weights": {"keywords": w_kw, "amount": w_amt},
        "score": {
            "keywords_0_10": score_kw_0_10,
            "amount_0_10": score_amt_0_10,
            "total_0_10": score_total_0_10,
            "display_max": display_max,
            "display_score": score_display,
        }
    }
    detail.update(det_kw)
    detail.update(det_amt)

    return score_display, detail
