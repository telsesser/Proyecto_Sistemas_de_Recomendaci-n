import requests
import time
import os
import json
import logging
from dotenv import load_dotenv
import pandas as pd
import fastparquet
from datetime import datetime
import signal
import sys
import gc  # Garbage collector

load_dotenv()

DEBUG = os.getenv("DEBUG", "False").lower() in ("true", "1", "t")
START_DATE = os.getenv("START_DATE", "2025-08-01")
PARQUET_ENGINE = "fastparquet"

# Configuración para prevenir memory leaks
MAX_ITEMS_PER_ENDPOINT = 1000  # Límite máximo por endpoint
BATCH_SIZE = 100  # Procesar en lotes
FORCE_GC_EVERY = 3  # Forzar garbage collection cada N repos

# Variables globales para estadísticas
stats = {
    "start_time": time.time(),
    "repos_processed": 0,
    "pages_processed": 0,
    "api_calls": 0,
    "last_repo": "",
    "errors": 0,
    "rate_limits": 0,
    "stars_fetched": 0,
    "contributors_fetched": 0,
    "forks_fetched": 0,
    "issues_fetched": 0,
}

# Configuración de logging
DATA_DIR = "data"
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)
LOG_PATH = os.path.join(DATA_DIR, "api_consumer.log")
STATUS_FILE = os.path.join(DATA_DIR, "status.json")

logging.basicConfig(
    level=logging.WARNING if not DEBUG else logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.FileHandler(LOG_PATH, encoding="utf-8"), logging.StreamHandler()],
)

if DEBUG:
    logging.getLogger().setLevel(logging.INFO)
    logging.info("Modo DEBUG activado")

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN2")
if not GITHUB_TOKEN:
    raise ValueError("❌ No se encontró GITHUB_TOKEN en el .env")

HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3.star+json",
}
BASE_URL = "https://api.github.com"


def update_status():
    """Actualiza archivo de estado con estadísticas actuales"""
    current_time = time.time()
    runtime = current_time - stats["start_time"]

    status_data = {
        "timestamp": datetime.now().isoformat(),
        "runtime_seconds": runtime,
        "runtime_formatted": f"{runtime//3600:.0f}h {(runtime%3600)//60:.0f}m {runtime%60:.0f}s",
        "repos_processed": stats["repos_processed"],
        "pages_processed": stats["pages_processed"],
        "api_calls": stats["api_calls"],
        "last_repo": stats["last_repo"],
        "errors": stats["errors"],
        "rate_limits": stats["rate_limits"],
        "avg_repos_per_hour": (
            stats["repos_processed"] / (runtime / 3600) if runtime > 0 else 0
        ),
        "avg_api_calls_per_minute": (
            stats["api_calls"] / (runtime / 60) if runtime > 0 else 0
        ),
        "stars_fetched": stats["stars_fetched"],
        "contributors_fetched": stats["contributors_fetched"],
        "forks_fetched": stats["forks_fetched"],
        "issues_fetched": stats["issues_fetched"],
    }

    with open(STATUS_FILE, "w") as f:
        json.dump(status_data, f, indent=2)


def log_progress():
    """Log periódico del progreso"""
    runtime = time.time() - stats["start_time"]
    logging.info(
        f"📊 PROGRESO: {stats['repos_processed']} repos | "
        f"Página {stats['pages_processed']} | "
        f"API calls: {stats['api_calls']} | "
        f"Runtime: {runtime//3600:.0f}h {(runtime%3600)//60:.0f}m | "
        f"Último: {stats['last_repo']}"
    )


def signal_handler(signum, frame):
    """Manejo de señales para cierre graceful"""
    logging.info("🛑 Señal de terminación recibida. Guardando estado...")
    update_status()
    logging.info(
        f"📊 RESUMEN FINAL: {stats['repos_processed']} repos procesados en {(time.time()-stats['start_time'])//60:.0f} minutos"
    )
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def github_request(url, params=None, retries=3):
    global stats
    stats["api_calls"] += 1

    for attempt in range(retries):
        resp = requests.get(url, headers=HEADERS, params=params, allow_redirects=True)

        limit = resp.headers.get("X-RateLimit-Limit")
        remaining = resp.headers.get("X-RateLimit-Remaining")
        reset = resp.headers.get("X-RateLimit-Reset")

        if remaining is not None and reset is not None:
            reset_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(reset)))
            if int(remaining) < 100:
                logging.warning(
                    f"⚠️ Rate limit bajo: {remaining}/{limit} (reset: {reset_time})"
                )

        if resp.status_code == 403 and resp.headers.get("X-RateLimit-Remaining") == "0":
            stats["rate_limits"] += 1
            reset_time = int(resp.headers.get("X-RateLimit-Reset", time.time() + 60))
            sleep_time = max(reset_time - int(time.time()), 60)
            logging.warning(
                f"💤 Rate limit alcanzado. Durmiendo {sleep_time} segundos..."
            )
            update_status()
            time.sleep(sleep_time)
            continue

        if resp.ok:
            return resp.json()
        else:
            stats["errors"] += 1
            logging.error(f"❌ Error en API: {resp.status_code} - {url}")

        time.sleep(2**attempt)

    stats["errors"] += 1
    return None


def get_paginated_generator(url, params=None, max_items=None):
    """
    Generador que yield páginas en lugar de acumular todo en memoria
    """
    page = 1
    items_fetched = 0

    while url and (max_items is None or items_fetched < max_items):
        if not params:
            params = {}
        params["per_page"] = 100

        logging.info(f"[get_paginated_generator] Página {page} - URL: {url}")
        resp = requests.get(url, headers=HEADERS, params=params)
        stats["api_calls"] += 1

        if not resp.ok:
            logging.error(f"❌ Error en paginación: status {resp.status_code}")
            stats["errors"] += 1
            break

        batch = resp.json()

        # Limitar items si se especifica max_items
        if max_items and items_fetched + len(batch) > max_items:
            batch = batch[: max_items - items_fetched]

        items_fetched += len(batch)
        yield batch

        if not batch or (max_items and items_fetched >= max_items):
            break

        # Link header para siguiente página
        links = resp.headers.get("Link", "")
        next_url = None
        for part in links.split(","):
            if 'rel="next"' in part:
                next_url = part[part.find("<") + 1 : part.find(">")]
        url = next_url
        page += 1


def save_data_batch(data_list, file_path, columns=None):
    """
    Guarda datos en lotes para evitar problemas de memoria con append
    """
    if not data_list:
        return

    df = pd.DataFrame(data_list)

    if os.path.exists(file_path):
        # Leer archivo existente y concatenar
        existing_df = pd.read_parquet(file_path, engine=PARQUET_ENGINE)
        combined_df = pd.concat([existing_df, df], ignore_index=True)
    else:
        combined_df = df

    # Escribir de vuelta
    combined_df.to_parquet(file_path, index=False, engine=PARQUET_ENGINE)

    # Limpiar memoria
    del df, combined_df
    if "existing_df" in locals():
        del existing_df
    gc.collect()


def get_repos_by_topic(topic="python", per_page=5, page=1):
    url = f"{BASE_URL}/search/repositories"
    params = {
        "q": f"topic:{topic} created:>{START_DATE}",
        "per_page": per_page,
        "page": page,
    }
    data = github_request(url, params)
    if not data:
        return []
    return data["items"]


def get_repo_data(owner, repo):
    global stats
    repo_key = f"{owner}/{repo}"
    stats["last_repo"] = repo_key

    logging.info(f"🔍 Procesando {repo_key}...")

    repo_json = github_request(f"{BASE_URL}/repos/{owner}/{repo}")
    if not repo_json:
        return None, [], [], [], []

    repo_info = {
        "repo": repo_key,
        "owner": owner,
        "is_fork": repo_json.get("fork", False),
        "stars": repo_json.get("stargazers_count", 0),
        "forks": repo_json.get("forks_count", 0),
        "watchers": repo_json.get("watchers_count", 0),
        "open_issues": repo_json.get("open_issues_count", 0),
        "has_issues": repo_json.get("has_issues", False),
        "has_projects": repo_json.get("has_projects", False),
        "has_wiki": repo_json.get("has_wiki", False),
        "has_pages": repo_json.get("has_pages", False),
        "has_downloads": repo_json.get("has_downloads", False),
        "created_at": repo_json.get("created_at", ""),
        "updated_at": repo_json.get("updated_at", ""),
        "topics": ",".join(repo_json.get("topics", [])),
    }

    # Procesar stargazers en lotes
    logging.info(f"⭐ Descargando stargazers de {repo_key}...")
    stars = []
    for batch in get_paginated_generator(
        f"{BASE_URL}/repos/{owner}/{repo}/stargazers", max_items=MAX_ITEMS_PER_ENDPOINT
    ):
        for s in batch:
            stars.append(
                {
                    "repo": repo_key,
                    "user": s["user"]["login"],
                    "timestamp": s["starred_at"],
                }
            )

        # Procesar lotes para evitar acumulación en memoria
        if len(stars) >= BATCH_SIZE:
            save_data_batch(stars, os.path.join(DATA_DIR, "stars.parquet"))
            stats["stars_fetched"] += len(stars)
            stars = []  # Limpiar lista

    # Guardar resto si queda algo
    if stars:
        save_data_batch(stars, os.path.join(DATA_DIR, "stars.parquet"))
        stats["stars_fetched"] += len(stars)

    logging.info(f"→ {stats['stars_fetched']} stargazers procesados")

    # Procesar contributors en lotes
    logging.info(f"👥 Descargando contributors de {repo_key}...")
    contributors = []
    for batch in get_paginated_generator(
        f"{BASE_URL}/repos/{owner}/{repo}/contributors",
        max_items=MAX_ITEMS_PER_ENDPOINT,
    ):
        for c in batch:
            contributors.append(
                {
                    "repo": repo_key,
                    "user": c["login"],
                    "commits": c["contributions"],
                }
            )

        if len(contributors) >= BATCH_SIZE:
            save_data_batch(
                contributors, os.path.join(DATA_DIR, "contributors.parquet")
            )
            stats["contributors_fetched"] += len(contributors)
            contributors = []

    if contributors:
        save_data_batch(contributors, os.path.join(DATA_DIR, "contributors.parquet"))
        stats["contributors_fetched"] += len(contributors)

    logging.info(f"→ {stats['contributors_fetched']} contributors procesados")

    # Procesar forks en lotes
    logging.info(f"🍴 Descargando forks de {repo_key}...")
    forks = []
    for batch in get_paginated_generator(
        f"{BASE_URL}/repos/{owner}/{repo}/forks", max_items=MAX_ITEMS_PER_ENDPOINT
    ):
        for f in batch:
            forks.append(
                {
                    "repo": repo_key,
                    "user": f["owner"]["login"],
                    "timestamp": f["created_at"],
                    "name": f["full_name"],
                    "url": f["html_url"],
                }
            )

        if len(forks) >= BATCH_SIZE:
            save_data_batch(forks, os.path.join(DATA_DIR, "forks.parquet"))
            stats["forks_fetched"] += len(forks)
            forks = []

    if forks:
        save_data_batch(forks, os.path.join(DATA_DIR, "forks.parquet"))
        stats["forks_fetched"] += len(forks)

    logging.info(f"→ {stats['forks_fetched']} forks procesados")

    # Procesar issues en lotes
    logging.info(f"🐞 Descargando issues de {repo_key}...")
    issues = []
    for batch in get_paginated_generator(
        f"{BASE_URL}/repos/{owner}/{repo}/issues",
        params={"state": "all"},
        max_items=MAX_ITEMS_PER_ENDPOINT,
    ):
        for i in batch:
            if "pull_request" not in i:  # Filtrar PRs
                issues.append(
                    {
                        "repo": repo_key,
                        "user": i["user"]["login"],
                        "timestamp": i["created_at"],
                        "url": i["html_url"],
                    }
                )

        if len(issues) >= BATCH_SIZE:
            save_data_batch(issues, os.path.join(DATA_DIR, "issues.parquet"))
            stats["issues_fetched"] += len(issues)
            issues = []

    if issues:
        save_data_batch(issues, os.path.join(DATA_DIR, "issues.parquet"))
        stats["issues_fetched"] += len(issues)

    logging.info(f"→ {stats['issues_fetched']} issues procesados")

    stats["repos_processed"] += 1
    logging.info(f"✅ {repo_key} completado ({stats['repos_processed']} repos totales)")

    # Forzar garbage collection periódicamente
    if stats["repos_processed"] % FORCE_GC_EVERY == 0:
        gc.collect()
        logging.info(
            f"🧹 Garbage collection ejecutado (repo #{stats['repos_processed']})"
        )

    return (
        repo_info,
        [],
        [],
        [],
        [],
    )  # Retornamos listas vacías ya que guardamos directamente


if __name__ == "__main__":
    logging.info("🚀 Iniciando scraper de GitHub...")
    logging.info(f"📅 Fecha de inicio: {START_DATE}")
    logging.info(
        f"🔧 Límites: {MAX_ITEMS_PER_ENDPOINT} items/endpoint, lotes de {BATCH_SIZE}"
    )

    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    repos_parquet_path = os.path.join(DATA_DIR, "repos.parquet")

    if os.path.exists(repos_parquet_path):
        existing_repos = pd.read_parquet(repos_parquet_path)
        existing_set = set(existing_repos["repo"])
        logging.info(f"📚 Cargados {len(existing_set)} repos existentes")
        del existing_repos  # Liberar memoria
        gc.collect()
    else:
        existing_set = set()
        logging.info("📝 Iniciando desde cero")

    page = 1
    last_progress_log = time.time()

    try:
        while True:
            logging.info(f"📖 Procesando página {page}...")
            repos = get_repos_by_topic("python", per_page=10, page=page)

            if not repos:
                logging.info("🏁 No hay más repositorios para procesar")
                break

            stats["pages_processed"] = page

            for r in repos:
                owner, name = r["owner"]["login"], r["name"]
                repo_key = f"{owner}/{name}"

                if repo_key in existing_set:
                    logging.info(f"⏭️ {repo_key} ya existe, saltando")
                    continue

                result = get_repo_data(owner, name)
                if result[0] is None:
                    continue

                repo_info = result[0]

                # Guardar repo info
                save_data_batch([repo_info], repos_parquet_path)
                existing_set.add(repo_key)

                update_status()

                if time.time() - last_progress_log > 300:
                    log_progress()
                    last_progress_log = time.time()

            page += 1
            time.sleep(2)

    except KeyboardInterrupt:
        logging.info("🛑 Interrupción manual detectada")
    except Exception as e:
        logging.error(f"💥 Error inesperado: {e}")
        stats["errors"] += 1
    finally:
        update_status()
        log_progress()
        logging.info("🏁 Script finalizado")
