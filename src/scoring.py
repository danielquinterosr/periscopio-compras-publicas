import re
import yaml
from typing import Any, Dict, List, Tuple, Optional


def load_rules(path: str = "config/rules.yml") -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _deep_merge(base: dict, override: dict) -> dict:
    """
    Merge simple (dicts anidados). override pisa base.
    """
    out = dict(base or {})
    for k, v in (override or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def get_effective_rules(all_rules: dict, source: Optional[str]) -> dict:
    """
    Soporta 2 formatos:
    A) Reglas antiguas (sin by_source): all_rules = {thresholds, weights, ...}
    B) Reglas nuevas:
       all_rules = {
         defaults: {... opcional ...},
         by_source: {
           licitaciones: {...},
           compra_agil: {...}
         }
       }
    """
    if not isinstance(all_rules, dict):
        return {}

    by_source = all_rules.get("by_source")
    if not isinstance(by_source, dict):
        # Formato antiguo (backward compatible)
        return all_rules

    defaults = all_rules.get("defaults") or {}
    src = (source or "").strip() or "licitaciones"
    profile = by_source.get(src) or by_source.get("licitaciones") or {}

    # Defaults + profile
    effective = _deep_merge(defaults, profile)
    effective["_rules_source"] = src
    return effective


def _compile_patterns(items: List[dict]) -> List[dict]:
    """
    Compila patrones regex de include/exclude definidos como:
      - pattern: "encuest(a|as)"
        weight: 3
        note: "..."
    """
    compiled: List[dict] = []
    for it in items or []:
        pat = (it.get("pattern") or "").strip()
        if not pat:
            continue
        flags = re.IGNORECASE | re.UNICODE
        compiled.append(
            {
                "pattern": pat,
                "weight": float(it.get("weight", 1)),
                "note": it.get("note", ""),
                "rx": re.compile(pat, flags),
            }
        )
    return compiled


def _score_keywords(text: str, rules: dict) -> Tuple[float, Dict[str, Any]]:
    """
    Retorna score_keywords en escala 0..10 y detalle.
    Lógica:
      - suma pesos de matches include
      - resta pesos de matches exclude
      - normaliza a 0..10 usando 'max_points' configurable
    """
    kw = (rules.get("keywords") or {})
    include = _compile_patterns(kw.get("include") or [])
    exclude = _compile_patterns(kw.get("exclude") or [])

    t = (text or "").strip()
    inc_hits: List[dict] = []
    exc_hits: List[dict] = []

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
    max_points = float(kw.get("max_points", 12))
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
    Asigna puntos por tramo de monto (+1..+N) y lo transforma a escala 0..10.
    """
    bands = (rules.get("amount_bands") or [])
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
    max_band_points = float(rules.get("amount_max_points", 7))

    score_0_10 = 0.0
    if max_band_points > 0:
        score_0_10 = max(0.0, min(10.0, (band_points / max_band_points) * 10.0))

    detail = {
        "amount": {
            "amount_clp": amt,
            "band": chosen.get("label") if chosen else None,
            "band_points": band_points,
            "score_0_10": score_0_10,
        }
    }
    return score_0_10, detail


def _blend_scores(
    score_kw_0_10: float,
    score_amt_0_10: float,
    w_kw: float,
    w_amt: float,
    gate_on_keywords: bool = True,
) -> Tuple[float, Dict[str, Any]]:
    """
    Combina keyword + monto en escala 0..10 con ponderadores.
    Reglas:
      - normaliza pesos
      - gating opcional: si keywords=0, el score total queda 0 (monto no rescata)
    """
    # Normalizar pesos si vienen mal
    s = float(w_kw) + float(w_amt)
    if s <= 0:
        w_kw, w_amt = 0.7, 0.3
        s = 1.0
    w_kw = float(w_kw) / s
    w_amt = float(w_amt) / s

    if gate_on_keywords and score_kw_0_10 <= 0:
        return 0.0, {"gate": {"enabled": True, "reason": "keywords_score_zero"}}

    total = (w_kw * score_kw_0_10) + (w_amt * score_amt_0_10)
    return total, {"gate": {"enabled": bool(gate_on_keywords), "reason": None}}


def total_score(
    text: str,
    amount_clp: Optional[float],
    rules: dict,
    source: Optional[str] = None,
) -> Tuple[int, Dict[str, Any]]:
    """
    Scoring ponderado por perfil (source):
      score_total_0_10 = w_kw*score_kw + w_amt*score_amt
    Luego se convierte a una escala entera para el dashboard (0..display_max).
    Con gating (por defecto): si keywords=0 → score final = 0.
    """
    eff = get_effective_rules(rules, source)

    weights = (eff.get("weights") or {})
    w_kw = float(weights.get("keywords", 0.7))
    w_amt = float(weights.get("amount", 0.3))

    score_kw_0_10, det_kw = _score_keywords(text, eff)
    score_amt_0_10, det_amt = _amount_band_points(amount_clp, eff)

    gate_on_keywords = bool((eff.get("thresholds") or {}).get("gate_on_keywords", True))

    score_total_0_10, det_gate = _blend_scores(
        score_kw_0_10=score_kw_0_10,
        score_amt_0_10=score_amt_0_10,
        w_kw=w_kw,
        w_amt=w_amt,
        gate_on_keywords=gate_on_keywords,
    )

    # Escala final para mostrar (entero)
    display_max = int((eff.get("thresholds") or {}).get("display_max_score", 20))
    if display_max <= 0:
        display_max = 20

    score_display = int(round((score_total_0_10 / 10.0) * display_max))
    score_display = max(0, min(display_max, score_display))

    # Detalle (auditable)
    # Nota: guardamos pesos normalizados tal como se usan en blend
    s = w_kw + w_amt
    if s <= 0:
        wkw_n, wamt_n = 0.7, 0.3
    else:
        wkw_n, wamt_n = w_kw / s, w_amt / s

    detail: Dict[str, Any] = {
        "rules_source": eff.get("_rules_source", source),
        "weights": {"keywords": wkw_n, "amount": wamt_n},
        "score": {
            "keywords_0_10": score_kw_0_10,
            "amount_0_10": score_amt_0_10,
            "total_0_10": score_total_0_10,
            "display_max": display_max,
            "display_score": score_display,
        },
    }
    detail.update(det_gate)
    detail.update(det_kw)
    detail.update(det_amt)

    return score_display, detail
