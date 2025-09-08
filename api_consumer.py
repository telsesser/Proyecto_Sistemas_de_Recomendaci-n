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

load_dotenv()

DEBUG = os.getenv("DEBUG", "False").lower() in ("true", "1", "t")
START_DATE = os.getenv("START_DATE", "2025-08-01")
PARQUET_ENGINE = "fastparquet"

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

# Configurar logging con más detalle
logging.basicConfig(
    level=logging.WARNING if not DEBUG else logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.FileHandler(LOG_PATH, encoding="utf-8"), logging.StreamHandler()],
)

if DEBUG:
    logging.getLogger().setLevel(logging.INFO)
    logging.info("Modo DEBUG activado")

# Cargar variables de entorno
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


# Registrar manejadores de señales
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def github_request(url, params=None, retries=3):
    global stats
    stats["api_calls"] += 1

    for attempt in range(retries):
        resp = requests.get(url, headers=HEADERS, params=params, allow_redirects=True)

        # Mostrar info de rate limit
        limit = resp.headers.get("X-RateLimit-Limit")
        remaining = resp.headers.get("X-RateLimit-Remaining")
        reset = resp.headers.get("X-RateLimit-Reset")

        if remaining is not None and reset is not None:
            reset_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(reset)))
            if int(remaining) < 100:  # Advertencia cuando quedan pocos requests
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
            update_status()  # Actualizar estado antes de dormir
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


def get_paginated(url, params=None):
    results = []
    page = 1
    while url:
        if not params:
            params = {}
        params["per_page"] = 100
        logging.info(f"[get_paginated] Página {page} - URL: {url}")
        resp = requests.get(url, headers=HEADERS, params=params)
        stats["api_calls"] += 1

        if not resp.ok:
            logging.error(f"❌ Error en paginación: status {resp.status_code}")
            stats["errors"] += 1
            break

        batch = resp.json()
        results.extend(batch)

        # Link header para siguiente página
        links = resp.headers.get("Link", "")
        next_url = None
        for part in links.split(","):
            if 'rel="next"' in part:
                next_url = part[part.find("<") + 1 : part.find(">")]
        url = next_url
        page += 1

    return results


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

    stars = []
    contributors = []
    forks = []
    issues = []

    # Stars
    logging.info(f"⭐ Descargando stargazers de {repo_key}...")
    stargazers = get_paginated(f"{BASE_URL}/repos/{owner}/{repo}/stargazers")
    logging.info(f"→ {len(stargazers)} stargazers encontrados")
    for s in stargazers:
        stars.append(
            {
                "repo": repo_key,
                "user": s["user"]["login"],
                "timestamp": s["starred_at"],
            }
        )
    stats["stars_fetched"] += len(stars)
    # Contributors
    logging.info(f"👥 Descargando contributors de {repo_key}...")
    contribs = get_paginated(f"{BASE_URL}/repos/{owner}/{repo}/contributors")
    logging.info(f"→ {len(contribs)} contributors encontrados")
    for c in contribs:
        contributors.append(
            {
                "repo": repo_key,
                "user": c["login"],
                "commits": c["contributions"],
            }
        )
    stats["contributors_fetched"] += len(contributors)
    # Forks
    logging.info(f"🍴 Descargando forks de {repo_key}...")
    repo_forks = get_paginated(f"{BASE_URL}/repos/{owner}/{repo}/forks")
    logging.info(f"→ {len(repo_forks)} forks encontrados")
    for f in repo_forks:
        forks.append(
            {
                "repo": repo_key,
                "user": f["owner"]["login"],
                "timestamp": f["created_at"],
                "name": f["full_name"],
                "url": f["html_url"],
            }
        )
    stats["forks_fetched"] += len(forks)
    # Issues
    logging.info(f"🐞 Descargando issues de {repo_key}...")
    repo_issues = get_paginated(
        f"{BASE_URL}/repos/{owner}/{repo}/issues", params={"state": "all"}
    )
    issues_count = 0
    for i in repo_issues:
        if "pull_request" not in i:
            issues.append(
                {
                    "repo": repo_key,
                    "user": i["user"]["login"],
                    "timestamp": i["created_at"],
                    "url": i["html_url"],
                }
            )
            issues_count += 1
    logging.info(f"→ {issues_count} issues encontrados (sin PRs)")
    stats["issues_fetched"] += len(issues)
    stats["repos_processed"] += 1
    logging.info(f"✅ {repo_key} completado ({stats['repos_processed']} repos totales)")
    return repo_info, stars, contributors, forks, issues


if __name__ == "__main__":
    logging.info("🚀 Iniciando scraper de GitHub...")
    logging.info(f"📅 Fecha de inicio: {START_DATE}")

    # Crear carpeta /data si no existe
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    # Cargar repos ya existentes
    repos_parquet_path = os.path.join(DATA_DIR, "repos.parquet")
    stars_parquet_path = os.path.join(DATA_DIR, "stars.parquet")
    contribs_parquet_path = os.path.join(DATA_DIR, "contributors.parquet")
    forks_parquet_path = os.path.join(DATA_DIR, "forks.parquet")
    issues_parquet_path = os.path.join(DATA_DIR, "issues.parquet")

    if os.path.exists(repos_parquet_path):
        existing_repos = pd.read_parquet(repos_parquet_path)
        existing_set = set(existing_repos["repo"])
        logging.info(f"📚 Cargados {len(existing_set)} repos existentes")
    else:
        existing_repos = pd.DataFrame(
            columns=[
                "repo",
                "owner",
                "is_fork",
                "stars",
                "forks",
                "watchers",
                "open_issues",
                "has_issues",
                "has_projects",
                "has_wiki",
                "has_pages",
                "has_downloads",
                "created_at",
                "updated_at",
                "topics",
            ]
        )
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
                if result[0] is None:  # Error en el repo
                    continue

                repo_info, stars, contribs, forks, issues = result

                # Guardar datos
                pd.DataFrame([repo_info]).to_parquet(
                    repos_parquet_path,
                    index=False,
                    engine=PARQUET_ENGINE,
                    append=True if os.path.exists(repos_parquet_path) else False,
                )
                existing_set.add(repo_key)

                if stars:
                    pd.DataFrame(stars).to_parquet(
                        stars_parquet_path,
                        index=False,
                        engine=PARQUET_ENGINE,
                        append=True if os.path.exists(stars_parquet_path) else False,
                    )
                if contribs:
                    pd.DataFrame(contribs).to_parquet(
                        contribs_parquet_path,
                        index=False,
                        engine=PARQUET_ENGINE,
                        append=True if os.path.exists(contribs_parquet_path) else False,
                    )
                if forks:
                    pd.DataFrame(forks).to_parquet(
                        forks_parquet_path,
                        index=False,
                        engine=PARQUET_ENGINE,
                        append=True if os.path.exists(forks_parquet_path) else False,
                    )
                if issues:
                    pd.DataFrame(issues).to_parquet(
                        issues_parquet_path,
                        index=False,
                        engine=PARQUET_ENGINE,
                        append=True if os.path.exists(issues_parquet_path) else False,
                    )

                # Actualizar estado cada repo
                update_status()

                # Log de progreso cada 5 minutos
                if time.time() - last_progress_log > 300:  # 5 minutos
                    log_progress()
                    last_progress_log = time.time()

            page += 1

            # Pequeña pausa entre páginas
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
