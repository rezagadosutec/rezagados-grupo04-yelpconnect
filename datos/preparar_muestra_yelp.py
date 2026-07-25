"""
preparar_muestra_yelp.py — SE EJECUTA EN TU LAPTOP (no en Databricks)

Genera una muestra del Yelp Open Dataset acotada a UNA ciudad, para que el
proyecto quepa cómodamente en la cuota de Databricks Free Edition.

Pasos previos:
  1. Descargar el Yelp Open Dataset: https://business.yelp.com/data/resources/open-dataset/
  2. Descomprimir el .tar en una carpeta local (obtendrán 5 archivos JSON Lines):
     yelp_academic_dataset_business.json / review.json / user.json / tip.json / checkin.json
  3. Ejecutar:  python preparar_muestra_yelp.py --src ./yelp_dataset --ciudad "Philadelphia"
  4. Subir los archivos generados en ./muestra/ al Volume:
     Catalog → workspace → yelp_bronze → raw → Upload
     (o arrastrarlos desde la UI de Databricks)

La muestra resultante mantiene integridad referencial:
business (ciudad elegida) → reviews/tips/checkins de esos negocios → users que escribieron.
"""
import argparse, json, gzip, os, random

MAX_REVIEWS = 400_000   # tope de seguridad para la cuota de Free Edition
MAX_USERS   = 150_000

def sample(src: str, ciudad: str, out: str, seed: int = 42):
    random.seed(seed)
    os.makedirs(out, exist_ok=True)

    # 1) Negocios de la ciudad elegida
    biz_ids = set()
    with open(f"{src}/yelp_academic_dataset_business.json", encoding="utf8") as f, \
         gzip.open(f"{out}/business.json.gz", "wt", encoding="utf8") as g:
        for line in f:
            r = json.loads(line)
            if r.get("city", "").strip().lower() == ciudad.strip().lower():
                biz_ids.add(r["business_id"])
                g.write(line)
    print(f"[business] {len(biz_ids)} negocios en {ciudad}")

    # 2) Reviews de esos negocios (con tope)
    user_ids, n_rev = set(), 0
    with open(f"{src}/yelp_academic_dataset_review.json", encoding="utf8") as f, \
         gzip.open(f"{out}/review.json.gz", "wt", encoding="utf8") as g:
        for line in f:
            r = json.loads(line)
            if r["business_id"] in biz_ids:
                g.write(line); user_ids.add(r["user_id"]); n_rev += 1
                if n_rev >= MAX_REVIEWS: break
    print(f"[review] {n_rev} reseñas · {len(user_ids)} usuarios únicos")

    # 3) Tips y check-ins de esos negocios
    for name in ("tip", "checkin"):
        n = 0
        with open(f"{src}/yelp_academic_dataset_{name}.json", encoding="utf8") as f, \
             gzip.open(f"{out}/{name}.json.gz", "wt", encoding="utf8") as g:
            for line in f:
                r = json.loads(line)
                if r["business_id"] in biz_ids:
                    g.write(line); n += 1
        print(f"[{name}] {n} registros")

    # 4) Usuarios que escribieron esas reseñas (con tope aleatorio si excede)
    if len(user_ids) > MAX_USERS:
        user_ids = set(random.sample(sorted(user_ids), MAX_USERS))
    n = 0
    with open(f"{src}/yelp_academic_dataset_user.json", encoding="utf8") as f, \
         gzip.open(f"{out}/user.json.gz", "wt", encoding="utf8") as g:
        for line in f:
            if json.loads(line)["user_id"] in user_ids:
                g.write(line); n += 1
    print(f"[user] {n} usuarios")
    print(f"\nListo. Subir el contenido de '{out}/' a /Volumes/workspace/yelp_bronze/raw/")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--src", required=True, help="Carpeta con los JSON del Yelp Open Dataset")
    p.add_argument("--ciudad", default="Philadelphia", help="Ciudad a muestrear")
    p.add_argument("--out", default="./muestra", help="Carpeta de salida")
    a = p.parse_args()
    sample(a.src, a.ciudad, a.out)
