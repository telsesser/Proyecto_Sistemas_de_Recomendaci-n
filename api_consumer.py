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

# NUEVA CONFIGURACIÓN: Tiempo para procesar usuarios (en segundos)
USER_PROCESSING_INTERVAL = 3600  # 1 hora = 3600 segundos
MAX_USERS_TO_PROCESS = 1000  # Máximo usuarios a procesar por ciclo


# Configuración de logging
DATA_DIR = "data"
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)
LOG_PATH = os.path.join(DATA_DIR, "api_consumer.log")
STATUS_FILE = os.path.join(DATA_DIR, "status.json")


# Variables globales para estadísticas
stats = None
if os.path.exists(STATUS_FILE):
    with open(STATUS_FILE, "r") as f:
        try:
            stats = json.load(f)
            stats["start_time"] = time.time()
        except json.JSONDecodeError:
            logging.error(
                "Error al leer el archivo de estado. Se utilizarán valores predeterminados."
            )
            stats = None

if not stats:
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
        "users_processed": 0,
        "last_user_processing": time.time(),
    }

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
        "users_processed": stats["users_processed"],
        "last_user_processing": datetime.fromtimestamp(
            stats["last_user_processing"]
        ).isoformat(),
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
        f"Users: {stats['users_processed']} | "
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
        stats["rate_limits"] = remaining
        if remaining and int(remaining) <= 0:
            reset_time = int(reset) if reset else time.time() + 60
            sleep_time = max(reset_time - int(time.time()), 60)
            logging.warning(f"💤 Rate limit en generador. Durmiendo {sleep_time}s...")
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
    global stats
    page = 1
    items_fetched = 0

    while url and (max_items is None or items_fetched < max_items):
        if not params:
            params = {}
        params["per_page"] = 100

        logging.info(f"[get_paginated_generator] Página {page} - URL: {url}")

        # Hacer la llamada directamente para obtener headers
        resp = requests.get(url, headers=HEADERS, params=params)
        stats["api_calls"] += 1

        # Control de rate limiting manual
        remaining = resp.headers.get("X-RateLimit-Remaining")
        reset = resp.headers.get("X-RateLimit-Reset")
        stats["rate_limits"] = remaining

        if remaining and int(remaining) <= 0:
            reset_time = int(reset) if reset else time.time() + 60
            sleep_time = max(reset_time - int(time.time()), 60)
            logging.warning(f"💤 Rate limit en generador. Durmiendo {sleep_time}s...")
            update_status()
            time.sleep(sleep_time)
            continue

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

        # Usar Link header para siguiente página
        links = resp.headers.get("Link", "")
        next_url = None
        for part in links.split(","):
            if 'rel="next"' in part:
                next_url = part[part.find("<") + 1 : part.find(">")]

        url = next_url
        page += 1

        # Seguridad: evitar bucles infinitos
        if page > 5000:
            logging.warning(
                f"⚠️ Alcanzado límite de seguridad de páginas, deteniendo..."
            )
            break


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
        "processed": True,  # NUEVA COLUMNA: marcar como procesado
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


def process_users_cycle():
    """
    NUEVA FUNCIÓN: Procesa el ciclo de usuarios
    1. Obtiene usuarios únicos de stars.parquet
    2. Los mueve a users.parquet
    3. Para cada usuario, obtiene sus repositorios estrellados
    4. Agrega nuevos repos a repos.parquet con processed=False
    """
    global stats

    logging.info("🔄 Iniciando ciclo de procesamiento de usuarios...")

    stars_path = os.path.join(DATA_DIR, "stars.parquet")
    users_path = os.path.join(DATA_DIR, "users.parquet")
    repos_path = os.path.join(DATA_DIR, "repos.parquet")

    # 1. Obtener usuarios únicos de stars.parquet
    if not os.path.exists(stars_path):
        logging.info("⚠️ No existe stars.parquet, saltando procesamiento de usuarios")
        return

    stars_df = pd.read_parquet(stars_path, engine=PARQUET_ENGINE)
    unique_users = stars_df["user"].unique()
    logging.info(f"👤 Encontrados {len(unique_users)} usuarios únicos en stars")

    # 2. Crear/actualizar users.parquet
    existing_users = set()
    if os.path.exists(users_path):
        users_df = pd.read_parquet(users_path, engine=PARQUET_ENGINE)
        existing_users = set(users_df["user"])
        logging.info(f"👥 Ya existen {len(existing_users)} usuarios procesados")
    else:
        users_df = pd.DataFrame(columns=["user", "processed"])

    # Agregar nuevos usuarios
    new_users = []
    for user in unique_users:
        if user not in existing_users:
            new_users.append({"user": user, "processed": False})

    if new_users:
        new_users_df = pd.DataFrame(new_users)
        if len(existing_users) > 0:
            users_df = pd.concat([users_df, new_users_df], ignore_index=True)
        else:
            users_df = new_users_df
        users_df.to_parquet(users_path, index=False, engine=PARQUET_ENGINE)
        logging.info(f"➕ Agregados {len(new_users)} usuarios nuevos")

    # 3. Procesar usuarios no procesados (limitado)
    unprocessed_users = users_df[users_df["processed"] == False]["user"].head(
        MAX_USERS_TO_PROCESS
    )
    logging.info(f"🔍 Procesando {len(unprocessed_users)} usuarios...")

    # Cargar repos existentes
    existing_repos = set()
    if os.path.exists(repos_path):
        repos_df = pd.read_parquet(repos_path, engine=PARQUET_ENGINE)
        existing_repos = set(repos_df["repo"])

    new_repos = []
    processed_users = []

    for user in unprocessed_users:
        logging.info(f"👤 Procesando usuario: {user}")

        # 4. Obtener repositorios estrellados por el usuario
        user_stars_data = github_request(f"{BASE_URL}/users/{user}/starred")
        if not user_stars_data:
            processed_users.append(user)
            stats["users_processed"] += 1
            continue

        for repo_data in user_stars_data:
            repo_key = repo_data["full_name"]

            if repo_key not in existing_repos:
                # Agregar nuevo repositorio con processed=False
                new_repo_info = {
                    "repo": repo_key,
                    "owner": repo_data["owner"]["login"],
                    "is_fork": repo_data.get("fork", False),
                    "stars": repo_data.get("stargazers_count", 0),
                    "forks": repo_data.get("forks_count", 0),
                    "watchers": repo_data.get("watchers_count", 0),
                    "open_issues": repo_data.get("open_issues_count", 0),
                    "has_issues": repo_data.get("has_issues", False),
                    "has_projects": repo_data.get("has_projects", False),
                    "has_wiki": repo_data.get("has_wiki", False),
                    "has_pages": repo_data.get("has_pages", False),
                    "has_downloads": repo_data.get("has_downloads", False),
                    "created_at": repo_data.get("created_at", ""),
                    "updated_at": repo_data.get("updated_at", ""),
                    "topics": ",".join(repo_data.get("topics", [])),
                    "processed": False,  # NUEVO REPO, NO PROCESADO
                }
                new_repos.append(new_repo_info)
                existing_repos.add(repo_key)

        processed_users.append(user)
        stats["users_processed"] += 1

        # Rate limiting protection
        time.sleep(1)

    # Guardar nuevos repos
    if new_repos:
        save_data_batch(new_repos, repos_path)
        logging.info(f"🆕 Agregados {len(new_repos)} nuevos repositorios para procesar")

    # Marcar usuarios como procesados
    if processed_users:
        users_df.loc[users_df["user"].isin(processed_users), "processed"] = True
        users_df.to_parquet(users_path, index=False, engine=PARQUET_ENGINE)
        logging.info(f"✅ Marcados {len(processed_users)} usuarios como procesados")

    stats["last_user_processing"] = time.time()
    logging.info("🔄 Ciclo de procesamiento de usuarios completado")


def should_process_users():
    """
    Determina si es tiempo de procesar usuarios
    """
    return (time.time() - stats["last_user_processing"]) >= USER_PROCESSING_INTERVAL


def get_unprocessed_repos():
    """
    NUEVA FUNCIÓN: Obtiene repositorios no procesados
    """
    repos_path = os.path.join(DATA_DIR, "repos.parquet")
    if not os.path.exists(repos_path):
        return []

    repos_df = pd.read_parquet(repos_path, engine=PARQUET_ENGINE)

    # Si no existe la columna processed, agregarla con True para repos existentes
    if "processed" not in repos_df.columns:
        repos_df["processed"] = True
        repos_df.to_parquet(repos_path, index=False, engine=PARQUET_ENGINE)
        return []

    unprocessed = repos_df[repos_df["processed"] == False]
    return [
        (row["owner"], row["repo"].split("/")[-1]) for _, row in unprocessed.iterrows()
    ]


def mark_repo_as_processed(repo_key):
    """
    NUEVA FUNCIÓN: Marca un repositorio como procesado
    """
    repos_path = os.path.join(DATA_DIR, "repos.parquet")
    if not os.path.exists(repos_path):
        return

    repos_df = pd.read_parquet(repos_path, engine=PARQUET_ENGINE)
    repos_df.loc[repos_df["repo"] == repo_key, "processed"] = True
    repos_df.to_parquet(repos_path, index=False, engine=PARQUET_ENGINE)


if __name__ == "__main__":
    logging.info("🚀 Iniciando scraper de GitHub con procesamiento de usuarios...")
    logging.info(f"📅 Fecha de inicio: {START_DATE}")
    logging.info(
        f"🔧 Límites: {MAX_ITEMS_PER_ENDPOINT} items/endpoint, lotes de {BATCH_SIZE}"
    )
    logging.info(
        f"👥 Procesamiento de usuarios cada {USER_PROCESSING_INTERVAL/60:.0f} minutos"
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
            # NUEVO: Verificar si es tiempo de procesar usuarios
            if should_process_users():
                process_users_cycle()

            # NUEVO: Procesar repositorios no procesados primero
            unprocessed_repos = get_unprocessed_repos()
            if unprocessed_repos:
                logging.info(
                    f"🔄 Procesando {len(unprocessed_repos)} repositorios pendientes..."
                )
                for owner, name in unprocessed_repos:
                    repo_key = f"{owner}/{name}"

                    result = get_repo_data(owner, name)
                    if result[0] is not None:
                        repo_info = result[0]
                        save_data_batch([repo_info], repos_parquet_path)
                        mark_repo_as_processed(repo_key)
                        existing_set.add(repo_key)

                    update_status()

                    if time.time() - last_progress_log > 300:
                        log_progress()
                        last_progress_log = time.time()

                continue  # Volver al inicio del loop para verificar más repos pendientes

            # Continuar con el flujo normal si no hay repos pendientes
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
