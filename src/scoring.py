import re
import yaml
from pathlib import Path

RULES_PATH = Path("config/rules.yml")

def load_rules() -> dict:
    with open(RULES_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def amount_score_clp(amount_clp: float | None) -> int:
    """
    Tramos (MM CLP):
      <=5: +1
      5-10: +2
      10-20: +3
      20-40: +4
      40-70: +5
      70-100: +6
      >100: +7
    """
    if amount_clp is None:
        return 0
    mm = amount_clp / 1_000_000
    if mm <= 5:
        return 1
    if mm <= 10:
        return 2
    if mm <= 20:
        return 3
    if mm <= 40:
        return 4
    if mm <= 70:
        return 5
    if mm <= 100:
        return 6
    return 7

def text_score(text: str, rules: dict) -> tuple[int, list[str]]:
    t = (text or "").lower()
    score = 0
    reasons: list[str] = []

    # inclusiones por bucket
    for bucket, spec in (rules.get("include") or {}).items():
        w = int(spec.get("weight", 0))
        for pat in spec.get("patterns", []):
            if re.search(pat, t, flags=re.IGNORECASE):
                score += w
                reasons.append(f"+{w}:{bucket}:{pat}")

    # exclusiones
    ex = rules.get("exclude") or {}
    exw = int(ex.get("weight", -6))
    for pat in ex.get("patterns", []):
        if re.search(pat, t, flags=re.IGNORECASE):
            score += exw
            reasons.append(f"{exw}:exclude:{pat}")

    return score, reasons

def total_score(text: str, amount_clp: float | None, rules: dict) -> tuple[int, dict]:
    ts, reasons = text_score(text, rules)
    ams = amount_score_clp(amount_clp)
    total = ts + ams
    detail = {"text_score": ts, "amount_score": ams, "reasons": reasons}
    return total, detail
