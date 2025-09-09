import aiohttp
import asyncio
import time
import os
import json
import logging
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime
import signal
import sys
import gc  # Garbage collector
import shutil
from typing import List, Dict, Optional, Tuple, AsyncGenerator
import sqlite3

load_dotenv()

DEBUG = os.getenv("DEBUG", "False").lower() in ("true", "1", "t")
START_DATE = os.getenv("START_DATE", "2025-01-01")

# Intentar importar fastparquet, usar pyarrow como fallback
try:
    import fastparquet

    PARQUET_ENGINE = "fastparquet"
except ImportError:
    PARQUET_ENGINE = "pyarrow"
    logging.warning("fastparquet no disponible, usando pyarrow")

# Configuración para prevenir memory leaks
MAX_ITEMS_PER_ENDPOINT = 3000  # Límite máximo por endpoint
BATCH_SIZE = 100  # Procesar en lotes
FORCE_GC_EVERY = 3  # Forzar garbage collection cada N repos

# Tiempo para procesar usuarios (en segundos)
USER_PROCESSING_INTERVAL = 360
MAX_USERS_TO_PROCESS = 1  # Máximo usuarios a procesar por ciclo

# Tiempo de espera para copias de seguridad (en segundos)
BACKUP_TIMEOUT = 3600  # 1 hora

# CONFIGURACIÓN ASÍNCRONA
MAX_CONCURRENT_REQUESTS = 5  # Reducido para evitar rate limiting
SEMAPHORE_LIMIT = 5  # Reducido para mayor estabilidad
REQUEST_DELAY = 0.5  # Aumentado para evitar rate limiting

# Configuración de logging
DATA_DIR = "data"
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)
LOG_PATH = os.path.join(DATA_DIR, "api_consumer.log")
STATUS_FILE = os.path.join(DATA_DIR, "status.json")
DB_PATH = os.path.join(DATA_DIR, "state.db")

# Variables globales para estadísticas
stats = None
if os.path.exists(STATUS_FILE):
    with open(STATUS_FILE, "r") as f:
        try:
            stats = json.load(f)
            stats["start_time"] = time.time()
            if stats.get("users_processed") is None:
                stats["users_processed"] = 0
                stats["last_user_processing"] = 0
            if stats.get("last_backup") is None:
                stats["last_backup"] = 0
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
        "stargazers_fetched": 0,
        "contributors_fetched": 0,
        "forks_fetched": 0,
        "issues_fetched": 0,
        "users_processed": 0,
        "last_user_processing": 0,
        "last_backup": 0,
    }

logging.basicConfig(
    level=logging.WARNING if not DEBUG else logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.FileHandler(LOG_PATH, encoding="utf-8"), logging.StreamHandler()],
)

if DEBUG:
    logging.getLogger().setLevel(logging.DEBUG)
    logging.info("Modo DEBUG activado")

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN2")
if not GITHUB_TOKEN:
    raise ValueError("❌ No se encontró GITHUB_TOKEN2 en el .env")

HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3.star+json",
    "User-Agent": "GitHub-Scraper-AsyncIO/1.0",
}
BASE_URL = "https://api.github.com"

# Semáforo global para controlar concurrencia
request_semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)


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
        "stargazers_fetched": stats["stargazers_fetched"],
        "contributors_fetched": stats["contributors_fetched"],
        "forks_fetched": stats["forks_fetched"],
        "issues_fetched": stats["issues_fetched"],
        "users_processed": stats["users_processed"],
        "last_user_processing": stats["last_user_processing"],
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


def backup_data_folder():
    """Crea una copia de seguridad de la carpeta de datos"""
    global stats
    backup_dir = f"{DATA_DIR}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    try:
        shutil.copytree(DATA_DIR, backup_dir)
        logging.info(f"💾 Copia de seguridad creada en {backup_dir}")
        stats["last_backup"] = time.time()
    except Exception as e:
        logging.error(f"❌ Error al crear copia de seguridad: {e}")


def get_db_conn():
    """Obtiene conexión a la base de datos SQLite"""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def init_db():
    """Inicializa la base de datos SQLite"""
    db_exists = os.path.exists(DB_PATH)

    conn = get_db_conn()
    cur = conn.cursor()

    # Crear tablas si no existen
    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS repos (
        repo TEXT PRIMARY KEY,
        owner TEXT,
        processed INTEGER DEFAULT 0,
        created_at TEXT,
        updated_at TEXT
    )
    """
    )

    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS users (
        user TEXT PRIMARY KEY,
        processed INTEGER DEFAULT 0,
        stars_count INTEGER DEFAULT 0
    )
    """
    )

    conn.commit()

    # Si la DB es nueva y existen los parquet → migrar datos
    if not db_exists:
        logging.info("Migrando datos de parquet a SQLite...")

        # Migrar users.parquet
        users_path = os.path.join(DATA_DIR, "users.parquet")
        if os.path.exists(users_path):
            try:
                users_df = pd.read_parquet(users_path, engine=PARQUET_ENGINE)
                if not users_df.empty:
                    cur.executemany(
                        "INSERT OR IGNORE INTO users(user, processed, stars_count) VALUES (?, ?, ?)",
                        users_df[["user", "processed", "stars_count"]].itertuples(
                            index=False, name=None
                        ),
                    )
                    logging.info(f"→ Migrados {len(users_df)} usuarios")
            except Exception as e:
                logging.error(f"Error migrando usuarios: {e}")

        # Migrar repos.parquet
        repos_path = os.path.join(DATA_DIR, "repos.parquet")
        if os.path.exists(repos_path):
            try:
                repos_df = pd.read_parquet(repos_path, engine=PARQUET_ENGINE)
                if not repos_df.empty:
                    cur.executemany(
                        """INSERT OR IGNORE INTO repos(repo, owner, processed, created_at, updated_at)
                           VALUES (?, ?, ?, ?, ?)""",
                        repos_df[
                            ["repo", "owner", "processed", "created_at", "updated_at"]
                        ].itertuples(index=False, name=None),
                    )
                    logging.info(f"→ Migrados {len(repos_df)} repositorios")
            except Exception as e:
                logging.error(f"Error migrando repos: {e}")

        conn.commit()

    conn.close()


def add_new_users(users: List[str]):
    """Añade nuevos usuarios a la base de datos"""
    if not users:
        return

    conn = get_db_conn()
    cur = conn.cursor()
    cur.executemany(
        "INSERT OR IGNORE INTO users(user, processed, stars_count) VALUES (?, 0, 0)",
        [(u,) for u in users],
    )
    conn.commit()
    conn.close()


def get_unprocessed_users_db(limit=MAX_USERS_TO_PROCESS):
    """Obtiene usuarios no procesados de la base de datos"""
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT user FROM users WHERE processed=0 LIMIT ?", (limit,))
    users = [r[0] for r in cur.fetchall()]
    conn.close()
    return users


def add_new_repos(repos: List[dict]):
    """Añade nuevos repositorios a la base de datos"""
    if not repos:
        return

    conn = get_db_conn()
    cur = conn.cursor()
    cur.executemany(
        """INSERT OR IGNORE INTO repos(repo, owner, processed, created_at, updated_at)
           VALUES (?, ?, 0, ?, ?)""",
        [(r["repo"], r["owner"], r["created_at"], r["updated_at"]) for r in repos],
    )
    conn.commit()
    conn.close()


def mark_repo_as_processed_db(repo_key: str):
    """Marca un repositorio como procesado en la base de datos"""
    conn = get_db_conn()
    conn.execute("UPDATE repos SET processed=1 WHERE repo=?", (repo_key,))
    conn.commit()
    conn.close()


def get_unprocessed_repos_db(limit=100):
    """Obtiene repositorios no procesados de la base de datos"""
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT repo FROM repos WHERE processed=0 LIMIT ?", (limit,))
    repos = cur.fetchall()
    conn.close()
    repos = [r[0] for r in repos]
    return repos


def add_user_stars_count(user: str, stars_count: int):
    """Actualiza el conteo de estrellas de un usuario"""
    conn = get_db_conn()
    conn.execute("UPDATE users SET stars_count=? WHERE user=?", (stars_count, user))
    conn.commit()
    conn.close()


def update_processed_users(users: List[str]):
    """Marca usuarios como procesados"""
    if not users:
        return

    conn = get_db_conn()
    cur = conn.cursor()
    cur.executemany(
        "UPDATE users SET processed=1 WHERE user=?",
        [(u,) for u in users],
    )
    conn.commit()
    conn.close()


def is_repo_processed(repo_key: str) -> bool:
    """Verifica si un repositorio ya ha sido procesado"""
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT processed FROM repos WHERE repo=?", (repo_key,))
    row = cur.fetchone()
    conn.close()
    return row is not None and row[0] == 1


async def handle_rate_limit(response_headers: dict) -> bool:
    """Maneja el rate limiting de forma centralizada"""
    remaining = response_headers.get("X-RateLimit-Remaining")
    reset = response_headers.get("X-RateLimit-Reset")

    if remaining:
        stats["rate_limits"] = int(remaining)

        if int(remaining) <= 10:
            reset_time = int(reset) if reset else time.time() + 60
            sleep_time = max(reset_time - int(time.time()), 60)
            logging.warning(
                f"💤 Rate limit bajo ({remaining}). Durmiendo {sleep_time}s..."
            )
            update_status()
            await asyncio.sleep(sleep_time)
            return True  # Indica que se durmió

    return False  # No se durmió


async def github_request_async(
    session: aiohttp.ClientSession,
    url: str,
    params: Optional[Dict] = None,
    retries: int = 3,
) -> Optional[Dict]:
    """Versión asíncrona de github_request con manejo mejorado de rate limiting"""
    global stats

    async with request_semaphore:
        stats["api_calls"] += 1

        for attempt in range(retries):
            try:
                await asyncio.sleep(REQUEST_DELAY)  # Delay para evitar rate limiting

                async with session.get(url, params=params) as resp:
                    # Manejar rate limiting
                    rate_limited = await handle_rate_limit(resp.headers)
                    if rate_limited:
                        continue  # Reintentar después del sleep

                    if resp.status == 200:
                        return await resp.json()
                    elif resp.status == 403:
                        error_text = await resp.text()
                        if "rate limit" in error_text.lower():
                            # Rate limit específico del response body
                            await asyncio.sleep(60)
                            continue
                    else:
                        stats["errors"] += 1
                        logging.error(f"❌ Error en API: {resp.status} - {url}")

            except asyncio.TimeoutError:
                logging.error(f"⏱️ Timeout en request: {url}")
                stats["errors"] += 1
            except Exception as e:
                logging.error(f"❌ Error en request: {e}")
                stats["errors"] += 1

            # Esperar antes del siguiente intento (backoff exponencial)
            await asyncio.sleep(min(2**attempt, 60))

        return None


async def get_paginated_async(
    session: aiohttp.ClientSession,
    url: str,
    params: Optional[Dict] = None,
    max_items: Optional[int] = None,
) -> AsyncGenerator[List[Dict], None]:
    """Generador asíncrono para paginación mejorado"""
    page = 1
    items_fetched = 0

    if not params:
        params = {}

    current_url = url

    while current_url and (max_items is None or items_fetched < max_items):
        page_params = params.copy()
        page_params["per_page"] = min(
            100, max_items - items_fetched if max_items else 100
        )

        logging.info(f"[Paginación] Página {page} - URL: {current_url}")

        async with session.get(current_url, params=page_params) as resp:
            stats["api_calls"] += 1

            # Manejar rate limiting
            rate_limited = await handle_rate_limit(resp.headers)
            if rate_limited:
                continue  # Reintentar la misma página

            batch = await resp.json()

            # Limitar items si se especifica max_items
            if max_items and items_fetched + len(batch) > max_items:
                batch = batch[: max_items - items_fetched]

            items_fetched += len(batch)
            yield batch

            if not batch or (max_items and items_fetched >= max_items):
                break

            # Usar Link header para siguiente página
            links = resp.headers.get("Link", "")
            current_url = None
            for part in links.split(","):
                if 'rel="next"' in part:
                    current_url = part[part.find("<") + 1 : part.find(">")]

            page += 1

            # Seguridad: evitar bucles infinitos
            if page > 5000:
                logging.warning(f"⚠️ Alcanzado límite de páginas, deteniendo...")
                break


def save_data_batch(data_list: List[Dict], file_path: str, columns=None):
    """
    Guarda datos en lotes de forma más eficiente en memoria
    """
    if not data_list:
        return

    try:
        df = pd.DataFrame(data_list)

        if os.path.exists(file_path):
            # Usar modo append si el engine lo soporta
            if PARQUET_ENGINE == "fastparquet":
                # fastparquet soporta append
                df.to_parquet(
                    file_path, index=False, engine=PARQUET_ENGINE, append=True
                )
            else:
                # pyarrow: leer, concatenar y escribir
                existing_df = pd.read_parquet(file_path, engine=PARQUET_ENGINE)
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                combined_df.to_parquet(file_path, index=False, engine=PARQUET_ENGINE)
                del existing_df, combined_df
        else:
            df.to_parquet(file_path, index=False, engine=PARQUET_ENGINE)

        # Si es stargazers, añadir usuarios únicos
        if file_path.endswith("stargazers.parquet") and "user" in df.columns:
            unique_users = df["user"].unique().tolist()
            add_new_users(unique_users)

        del df
        gc.collect()

    except Exception as e:
        logging.error(f"❌ Error guardando datos en {file_path}: {e}")


async def get_repos_by_topic_async(
    session: aiohttp.ClientSession,
    topic: str = "python",
    per_page: int = 5,
    page: int = 1,
) -> List[Dict]:
    """Versión asíncrona de get_repos_by_topic"""
    url = f"{BASE_URL}/search/repositories"
    params = {
        "q": f"topic:{topic} created:>{START_DATE}",
        "per_page": per_page,
        "page": page,
    }
    data = await github_request_async(session, url, params)
    if not data:
        return []
    return data.get("items", [])


async def fetch_repo_endpoint(
    session: aiohttp.ClientSession,
    endpoint_url: str,
    repo_key: str,
    endpoint_name: str,
    max_items: int = MAX_ITEMS_PER_ENDPOINT,
) -> List[Dict]:
    """Función auxiliar para obtener datos de un endpoint específico de forma asíncrona"""
    data = []

    async for batch in get_paginated_async(session, endpoint_url, max_items=max_items):
        for item in batch:
            try:
                if endpoint_name == "stargazers":
                    data.append(
                        {
                            "repo": repo_key,
                            "user": item["user"]["login"],
                            "timestamp": item.get("starred_at", ""),
                        }
                    )
                elif endpoint_name == "contributors":
                    data.append(
                        {
                            "repo": repo_key,
                            "user": item["login"],
                            "commits": item.get("contributions", 0),
                        }
                    )
                elif endpoint_name == "forks":
                    data.append(
                        {
                            "repo": repo_key,
                            "user": item["owner"]["login"],
                            "timestamp": item.get("created_at", ""),
                            "name": item.get("full_name", ""),
                            "url": item.get("html_url", ""),
                        }
                    )
                elif endpoint_name == "issues":
                    if "pull_request" not in item:  # Filtrar PRs
                        data.append(
                            {
                                "repo": repo_key,
                                "user": item["user"]["login"],
                                "timestamp": item.get("created_at", ""),
                                "url": item.get("html_url", ""),
                            }
                        )
            except KeyError as e:
                logging.error(f"❌ Error procesando item en {endpoint_name}: {e}")
                continue

        # Procesar en lotes para evitar acumulación en memoria
        if len(data) >= BATCH_SIZE:
            save_data_batch(data, os.path.join(DATA_DIR, f"{endpoint_name}.parquet"))
            stats[f"{endpoint_name}_fetched"] += len(data)
            data = []  # Limpiar lista

    # Guardar resto si queda algo
    if data:
        save_data_batch(data, os.path.join(DATA_DIR, f"{endpoint_name}.parquet"))
        stats[f"{endpoint_name}_fetched"] += len(data)

    return []  # Retornamos lista vacía ya que guardamos directamente


async def get_repo_data_async(
    session: aiohttp.ClientSession, owner: str, repo: str
) -> Tuple[Optional[Dict], List, List, List, List]:
    """Versión asíncrona optimizada de get_repo_data"""
    global stats
    repo_key = f"{owner}/{repo}"
    stats["last_repo"] = repo_key

    logging.info(f"🔍 Procesando {repo_key} (async)...")

    # Obtener información básica del repositorio
    repo_json = await github_request_async(session, f"{BASE_URL}/repos/{owner}/{repo}")
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
        "processed": True,
    }

    # Crear tareas concurrentes para todos los endpoints
    endpoints = [
        (f"{BASE_URL}/repos/{repo_key}/stargazers", "stargazers"),
        (f"{BASE_URL}/repos/{repo_key}/contributors", "contributors"),
        (f"{BASE_URL}/repos/{repo_key}/forks", "forks"),
        (f"{BASE_URL}/repos/{repo_key}/issues", "issues"),
    ]

    # Ejecutar todas las tareas concurrentemente
    logging.info(
        f"🚀 Procesando {len(endpoints)} endpoints concurrentemente para {repo_key}..."
    )

    tasks = [
        fetch_repo_endpoint(session, endpoint_url, repo_key, endpoint_name)
        for endpoint_url, endpoint_name in endpoints
    ]

    # Esperar a que todas las tareas se completen
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Manejar excepciones
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            endpoint_name = endpoints[i][1]
            logging.error(
                f"❌ Error en endpoint {endpoint_name} para {repo_key}: {result}"
            )

    stats["repos_processed"] += 1
    logging.info(
        f"✅ {repo_key} completado (async) - ({stats['repos_processed']} repos totales)"
    )

    # Forzar garbage collection periódicamente
    if stats["repos_processed"] % FORCE_GC_EVERY == 0:
        gc.collect()
        logging.info(
            f"🧹 Garbage collection ejecutado (repo #{stats['repos_processed']})"
        )

    return repo_info, [], [], [], []


async def process_users_cycle_async(session: aiohttp.ClientSession):
    """Versión asíncrona del procesamiento de usuarios"""
    global stats

    logging.info("🔄 Iniciando ciclo de procesamiento de usuarios (async)...")

    # Obtener usuarios únicos no procesados de la base de datos
    unprocessed_users = get_unprocessed_users_db(MAX_USERS_TO_PROCESS)

    if not unprocessed_users:
        logging.info("ℹ️ No hay usuarios pendientes para procesar")
        return

    logging.info(f"👤 Procesando {len(unprocessed_users)} usuarios...")

    # Procesar usuarios concurrentemente
    async def process_single_user(user: str) -> Tuple[str, List[Dict], int]:
        """Procesa un usuario individual de forma asíncrona"""
        logging.info(f"👤 Procesando usuario: {user}")

        new_repos = []
        user_starred_count = 0

        try:
            # Obtener repositorios estrellados por el usuario usando paginación asíncrona
            async for batch in get_paginated_async(
                session, f"{BASE_URL}/users/{user}/starred"
            ):
                if not batch:
                    continue

                for item in batch:
                    user_starred_count += 1
                    starred_repo = item["repo"]
                    repo_key = starred_repo["full_name"]

                    new_repo_info = {
                        "repo": repo_key,
                        "owner": starred_repo["owner"]["login"],
                        "is_fork": starred_repo.get("fork", False),
                        "stars": starred_repo.get("stargazers_count", 0),
                        "forks": starred_repo.get("forks_count", 0),
                        "watchers": starred_repo.get("watchers_count", 0),
                        "open_issues": starred_repo.get("open_issues_count", 0),
                        "has_issues": starred_repo.get("has_issues", False),
                        "has_projects": starred_repo.get("has_projects", False),
                        "has_wiki": starred_repo.get("has_wiki", False),
                        "has_pages": starred_repo.get("has_pages", False),
                        "has_downloads": starred_repo.get("has_downloads", False),
                        "created_at": starred_repo.get("created_at", ""),
                        "updated_at": starred_repo.get("updated_at", ""),
                        "topics": ",".join(starred_repo.get("topics", [])),
                        "processed": False,
                    }
                    new_repos.append(new_repo_info)

        except Exception as e:
            logging.error(f"❌ Error procesando usuario {user}: {e}")

        return user, new_repos, user_starred_count

    # Ejecutar procesamiento de usuarios con límite de concurrencia
    semaphore = asyncio.Semaphore(3)  # Límite más conservador para usuarios

    async def bounded_process_user(user: str):
        async with semaphore:
            return await process_single_user(user)

    user_tasks = [bounded_process_user(user) for user in unprocessed_users]
    user_results = await asyncio.gather(*user_tasks, return_exceptions=True)

    # Procesar resultados
    all_new_repos = []
    processed_users = []

    for result in user_results:
        if isinstance(result, Exception):
            logging.error(f"❌ Error procesando usuario: {result}")
            continue

        user, new_repos, stars_count = result
        all_new_repos.extend(new_repos)
        processed_users.append(user)

        # Actualizar stars_count para el usuario
        add_user_stars_count(user, stars_count)
        stats["users_processed"] += 1

    # Guardar nuevos repos en la base de datos
    if all_new_repos:
        add_new_repos(all_new_repos)
        logging.info(
            f"🆕 Agregados {len(all_new_repos)} nuevos repositorios para procesar"
        )

    # Marcar usuarios como procesados
    if processed_users:
        update_processed_users(processed_users)
        logging.info(f"✅ Marcados {len(processed_users)} usuarios como procesados")

    stats["last_user_processing"] = time.time()
    logging.info("🔄 Ciclo de procesamiento de usuarios completado (async)")


def should_process_users():
    """Determina si es tiempo de procesar usuarios"""
    return (time.time() - stats["last_user_processing"]) >= USER_PROCESSING_INTERVAL


async def process_repos_concurrently(
    session: aiohttp.ClientSession,
    repos_to_process: List[str],
):
    """Procesa múltiples repositorios concurrentemente"""

    async def process_single_repo(repo_key: str):
        repo_parts = repo_key.split("/")
        owner = repo_parts[0]
        name = repo_parts[1]
        if is_repo_processed(repo_key):
            logging.info(f"ℹ️ {repo_key} ya procesado, saltando...")
            return True
        try:
            result = await get_repo_data_async(session, owner, name)
            if result[0] is not None:
                repo_info = result[0]
                save_data_batch([repo_info], os.path.join(DATA_DIR, "repos.parquet"))
                mark_repo_as_processed_db(repo_key)

            return True
        except Exception as e:
            logging.error(f"❌ Error procesando repo {repo_key}: {e}")
            return False

    # Procesar repos en lotes concurrentes
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async def bounded_process(name: str):
        async with semaphore:
            return await process_single_repo(name)

    tasks = [bounded_process(name) for name in repos_to_process]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    successful = sum(1 for r in results if r is True)
    logging.info(
        f"✅ Procesados {successful}/{len(repos_to_process)} repositorios concurrentemente"
    )


async def main():
    """Función principal asíncrona"""
    logging.info("🚀 Iniciando scraper asíncrono de GitHub...")
    logging.info(f"📅 Fecha de inicio: {START_DATE}")
    logging.info(
        f"🔧 Límites: {MAX_ITEMS_PER_ENDPOINT} items/endpoint, lotes de {BATCH_SIZE}"
    )
    logging.info(
        f"👥 Procesamiento de usuarios cada {USER_PROCESSING_INTERVAL/60:.0f} minutos"
    )
    logging.info(f"🚀 Máximo requests concurrentes: {MAX_CONCURRENT_REQUESTS}")

    # Inicializar base de datos
    init_db()

    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    last_progress_log = time.time()

    # Configurar sesión HTTP asíncrona con timeouts más generosos
    timeout = aiohttp.ClientTimeout(total=60, connect=15)
    connector = aiohttp.TCPConnector(
        limit=MAX_CONCURRENT_REQUESTS,
        limit_per_host=SEMAPHORE_LIMIT,
        ttl_dns_cache=300,
        use_dns_cache=True,
    )

    try:
        async with aiohttp.ClientSession(
            headers=HEADERS, timeout=timeout, connector=connector
        ) as session:

            while True:
                try:
                    # Verificar si es tiempo de crear backup
                    if time.time() - stats["last_backup"] > BACKUP_TIMEOUT:
                        backup_data_folder()

                    # Verificar si es tiempo de procesar usuarios
                    if should_process_users():
                        await process_users_cycle_async(session)

                    # Procesar repositorios no procesados primero (usar BD si está disponible)
                    unprocessed_repos = get_unprocessed_repos_db(50)

                    if unprocessed_repos:
                        logging.info(
                            f"🔄 Procesando {len(unprocessed_repos)} repositorios pendientes concurrentemente..."
                        )

                        # Procesar en lotes pequeños para no sobrecargar
                        batch_size = min(
                            5, len(unprocessed_repos)
                        )  # Lotes más pequeños
                        for i in range(0, len(unprocessed_repos), batch_size):
                            batch = unprocessed_repos[i : i + batch_size]
                            logging.info(f"🚀 Procesando repositorios: {batch}")
                            await process_repos_concurrently(session, batch)

                            update_status()

                            if time.time() - last_progress_log > 300:
                                log_progress()
                                last_progress_log = time.time()

                            # Pausa más larga entre lotes para ser más respetuoso con la API
                            await asyncio.sleep(5)

                        continue  # Volver al inicio del loop para verificar más repos pendientes

                    # Continuar con el flujo normal si no hay repos pendientes
                    logging.info(
                        f"📖 Procesando página {stats['pages_processed'] + 1}..."
                    )
                    repos = await get_repos_by_topic_async(
                        session,
                        "python",
                        per_page=10,
                        page=stats["pages_processed"] + 1,
                    )

                    if not repos:
                        logging.info("🏁 No hay más repositorios para procesar")
                        break

                    # Filtrar repos que ya existen
                    new_repos = []
                    for r in repos:
                        owner, name = r["owner"]["login"], r["name"]
                        name = f"{owner}/{name}"
                        r["repo"] = name
                        r["owner"] = owner
                        r["created_at"] = r.get("created_at", "")
                        r["updated_at"] = r.get("updated_at", "")
                        new_repos.append(name)

                    # Procesar nuevos repos concurrentemente
                    add_new_repos(repos)
                    if new_repos:
                        logging.info(
                            f"🚀 Procesando {len(new_repos)} nuevos repositorios concurrentemente..."
                        )
                        await process_repos_concurrently(session, new_repos)

                    update_status()

                    if time.time() - last_progress_log > 300:
                        log_progress()
                        last_progress_log = time.time()

                    stats["pages_processed"] += 1
                    await asyncio.sleep(3)  # Pausa más larga entre páginas

                except Exception as e:
                    logging.error(f"💥 Error en loop principal: {e}")
                    stats["errors"] += 1
                    await asyncio.sleep(30)  # Pausa más larga en caso de error
                    continue

    except KeyboardInterrupt:
        logging.info("🛑 Interrupción manual detectada")
    except Exception as e:
        logging.error(f"💥 Error inesperado: {e}")
        stats["errors"] += 1
        raise
    finally:
        update_status()
        log_progress()
        logging.info("🏁 Script finalizado")


if __name__ == "__main__":
    # Ejecutar el loop asíncrono principal
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("🛑 Interrupción recibida, cerrando...")
    except Exception as e:
        logging.error(f"💥 Error crítico: {e}")
        raise
