import os
from datetime import date, timedelta
import requests

API_URL = "https://api.buscador.mercadopublico.cl/compra-agil"


def build_params(date_from: str, date_to: str) -> dict:
    return {
        "action": "download-excel",
        "date_from": date_from,
        "date_to": date_to,
        "order_by": "recent",
        "status": "2",
    }


def _is_xlsx_bytes(b: bytes) -> bool:
    # XLSX es ZIP => firma PK
    return len(b) > 4 and b[:2] == b"PK"


def download_via_presigned_url(out_path: str, api_key: str, date_from: str, date_to: str) -> None:
    headers = {
        "accept": "application/json, text/plain, */*",
        "origin": "https://buscador.mercadopublico.cl",
        "referer": "https://buscador.mercadopublico.cl/",
        "user-agent": os.environ.get(
            "UA",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36",
        ),
        "x-api-key": api_key,
    }

    params = build_params(date_from, date_to)

    # 1) pedir presigned_url
    r = requests.get(API_URL, headers=headers, params=params, timeout=180)
    r.raise_for_status()

    ct = (r.headers.get("content-type") or "").lower()
    if "application/json" not in ct:
        # en caso de que cambie y devuelva binario directo, lo soportamos igual
        content = r.content or b""
        if not _is_xlsx_bytes(content):
            raise RuntimeError(f"Respuesta inesperada (no JSON, no XLSX). content-type={ct}")
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        with open(out_path, "wb") as f:
            f.write(content)
        return

    data = r.json()
    presigned_url = data.get("presigned_url") or data.get("url") or data.get("download_url") or data.get("downloadUrl")
    if not presigned_url:
        raise RuntimeError(f"No encontré presigned_url en JSON. keys={list(data.keys())[:30]}")

    # 2) descargar XLSX desde S3 (no requiere headers especiales; el token va en la URL)
    r2 = requests.get(presigned_url, timeout=180)
    r2.raise_for_status()

    content2 = r2.content or b""
    if not _is_xlsx_bytes(content2):
        # diagnóstico útil
        preview = content2[:200].decode("utf-8", errors="replace")
        raise RuntimeError(f"Descarga desde presigned_url no parece XLSX. preview={preview}")

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "wb") as f:
        f.write(content2)

    if os.path.getsize(out_path) < 50_000:
        raise RuntimeError(f"Archivo XLSX descargado demasiado pequeño (sospechoso): {out_path}")


def main():
    out_path = os.environ.get("COMPRA_AGIL_XLSX_OUT", "data/compra_agil.xlsx")

    api_key = (os.environ.get("MP_COMPRA_AGIL_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("Falta MP_COMPRA_AGIL_API_KEY. Usa el valor de x-api-key que viste en DevTools/cURL.")

    days = int(os.environ.get("COMPRA_AGIL_DAYS", "30"))
    to_dt = date.today()
    from_dt = to_dt - timedelta(days=days)

    date_from = os.environ.get("COMPRA_AGIL_DATE_FROM", from_dt.isoformat())
    date_to = os.environ.get("COMPRA_AGIL_DATE_TO", to_dt.isoformat())

    download_via_presigned_url(out_path, api_key, date_from, date_to)
    print(f"OK: Compra Ágil Excel descargado → {out_path} ({date_from}..{date_to})")


if __name__ == "__main__":
    main()
