#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Descarga exactamente los CSV del anio 2025 a partir de enlaces de API CKAN.
Salida: ./bcn_meteo_csv
"""

from __future__ import annotations

import json
import re
import shutil
from pathlib import Path
from urllib.parse import parse_qs, urlparse, urlencode
from urllib.request import Request, urlopen


ACTION_ROOT = "https://opendata-ajuntament.barcelona.cat/data/api/action"
OUT_DIR = Path(__file__).resolve().parent / "bcn_meteo_csv"
YEAR = "2025"

# Sustituye o amplia esta lista con tus 3 enlaces de API.
API_LINKS = [
	"https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=8e7388ee-ffbd-446e-a8ad-c427a99da48d&limit=5",
	"https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=066d46b1-25be-4f08-b0e0-5a233714bda2&limit=5",
	"https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search?resource_id=00904de2-8660-4c41-92e3-66e7c87265be&limit=5",
]


def safe_filename(name: str) -> str:
	return re.sub(r"[^\w\-.]+", "_", name.strip())


def extract_resource_id(api_link: str) -> str:
	parsed = urlparse(api_link)
	qs = parse_qs(parsed.query)
	rid = (qs.get("resource_id") or [""])[0].strip()
	if not rid:
		raise ValueError(f"No se encontro resource_id en: {api_link}")
	return rid


def ckan_resource_show(resource_id: str) -> dict:
	url = f"{ACTION_ROOT}/resource_show?{urlencode({'id': resource_id})}"
	req = Request(url, headers={"User-Agent": "bcn-meteorologics/1.0", "Accept": "application/json"})
	with urlopen(req, timeout=60) as resp:
		payload = json.loads(resp.read().decode("utf-8"))
	if not payload.get("success"):
		raise RuntimeError(f"resource_show fallo para {resource_id}: {payload}")
	return payload["result"]


def is_target_2025_csv(resource: dict) -> bool:
	name = str(resource.get("name") or "")
	fmt = str(resource.get("format") or "")
	url = str(resource.get("url") or "")
	text = f"{name} {url}".lower()
	return YEAR in text and "csv" in fmt.lower()


def download_csv(url: str, out_path: Path) -> None:
	req = Request(url, headers={"User-Agent": "bcn-meteorologics/1.0"})
	with urlopen(req, timeout=120) as src, out_path.open("wb") as dst:
		shutil.copyfileobj(src, dst)


def main() -> int:
	OUT_DIR.mkdir(parents=True, exist_ok=True)
	downloaded = 0

	for idx, link in enumerate(API_LINKS, start=1):
		try:
			rid = extract_resource_id(link)
			resource = ckan_resource_show(rid)
			if not is_target_2025_csv(resource):
				print(f"[{idx}] Omitido (no es CSV {YEAR}): {rid}")
				continue

			name = str(resource.get("name") or f"{rid}.csv")
			filename = safe_filename(name)
			if not filename.lower().endswith(".csv"):
				filename += ".csv"

			url = str(resource.get("url") or "").strip()
			if not url:
				print(f"[{idx}] Omitido (sin URL de descarga): {rid}")
				continue

			out_path = OUT_DIR / filename
			print(f"[{idx}] Descargando: {filename}")
			download_csv(url, out_path)
			downloaded += 1
		except Exception as exc:
			print(f"[{idx}] Error: {exc}")

	print(f"Completado. CSV {YEAR} descargados: {downloaded}. Carpeta: {OUT_DIR}")
	return 0 if downloaded else 1


if __name__ == "__main__":
	raise SystemExit(main())