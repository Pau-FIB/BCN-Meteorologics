import os, re, json
from urllib.request import urlopen, Request
from urllib.parse import urlencode

ACTION_ROOT = "https://opendata-ajuntament.barcelona.cat/data/api/action"
DATASET_ID  = "mesures-estacions-meteorologiques"
BASE_YEAR   = 2023          # condición: año > BASE_YEAR
OUT_DIR     = "bcn_meteo_csv"

os.makedirs(OUT_DIR, exist_ok=True)

def ckan(action, **params):
    url = f"{ACTION_ROOT}/{action}?{urlencode(params)}"
    with urlopen(url) as r:
        data = json.load(r)
    if not data.get("success"):
        raise RuntimeError(data)
    return data["result"]

def best_year(text):
    years = [int(y) for y in re.findall(r"(?:19|20)\d{2}", text)]
    return max(years) if years else None

def download(url, path):
    req = Request(url, headers={"User-Agent": "bcn-opendata-csv-downloader/1.0"})
    with urlopen(req) as r, open(path, "wb") as f:
        while True:
            chunk = r.read(1024 * 1024)
            if not chunk:
                break
            f.write(chunk)

pkg = ckan("package_show", id=DATASET_ID)

n = 0
for res in pkg.get("resources", []):
    name = res.get("name") or res.get("title") or ""
    url  = res.get("url") or ""
    fmt  = (res.get("format") or "").lower()

    if not (("csv" in fmt) or url.lower().endswith(".csv")):
        continue

    y = best_year(f"{name} {url}")
    if not (y and y >= BASE_YEAR):
        continue

    fname = re.sub(r"[^\w\-.]+", "_", (name or res["id"]))[:180] + ".csv"
    path = os.path.join(OUT_DIR, fname)

    print(f"Downloading ({y}): {name} -> {path}")
    download(url, path)
    n += 1

print(f"Done. CSV downloaded: {n} in ./{OUT_DIR}")