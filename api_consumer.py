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
import traceback
import orjson


load_dotenv()

DEBUG = os.getenv("DEBUG", "False").lower() in ("true", "1", "t")
START_DATE = os.getenv("START_DATE", "2025-01-01")

# Configuración para prevenir memory leaks
MAX_ITEMS_PER_ENDPOINT = 3000  # Límite máximo por endpoint
BATCH_SIZE = 100  # Procesar en lotes
FORCE_GC_EVERY = 3  # Forzar garbage collection cada N repos

# Tiempo para procesar usuarios (en segundos)
USER_PROCESSING_INTERVAL = 360
MAX_USERS_TO_PROCESS = 4  # Máximo usuarios a procesar por ciclo

# Tiempo de espera para copias de seguridad (en segundos)
BACKUP_TIMEOUT = 3600  # 1 hora

# CONFIGURACIÓN ASÍNCRONA
MAX_CONCURRENT_REQUESTS = 4  # Máximo requests concurrentes
SEMAPHORE_LIMIT = 2  # limite de conexiones simultaneas a la API
UNPROCESSED_REPOS_TO_PROCESS = 120  # Número de repositorios pendientes para procesar
REPOS_BATCH_SIZE = 6  # Número de repos a procesar en paralelo
SEMAPHORE_REPO_LIMIT = 3  # Límite DE procesamiento de repos en simultáneo
SEMAPHORE_USER_LIMIT = 2  # Límite de procesamiento de usuarios en simultáneo
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
            stats["repos_processed"] = 0
            stats["pages_processed"] = 0  # paginas de pantalla principal de github
            stats["pages_fetched"] = 0  # paginas de pagination
            stats["api_calls"] = 0
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
        "all_users": 0,
        "last_repo": "",
        "errors": 0,
        "rate_limits": 0,
        "pages_fetched": 0,
        "repos_fetched": 0,
        "all_repos_processed": 0,
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

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_TOKEN2 = os.getenv("GITHUB_TOKEN2")


headers = {
    "header1": {
        "header": {
            "Authorization": f"Bearer {GITHUB_TOKEN}",
            "Accept": "application/vnd.github.v3.star+json",
            "User-Agent": "GitHub-Scraper-AsyncIO/1.0",
        },
        "rate_limit": 5000,
        "reset_time": 0,
    },
    "header2": {
        "header": {
            "Authorization": f"Bearer {GITHUB_TOKEN2}",
            "Accept": "application/vnd.github.v3.star+json",
            "User-Agent": "GitHub-Scraper-AsyncIO/1.0",
        },
        "rate_limit": 5000,
        "reset_time": 0,
    },
}
BASE_URL = "https://api.github.com"

if not GITHUB_TOKEN:
    raise ValueError("❌ No se encontró GITHUB_TOKEN2 en el .env")

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
        "pages_fetched": stats["pages_fetched"],
        "repos_fetched": stats["repos_fetched"],
        "all_repos_processed": stats["all_repos_processed"],
        "api_calls": stats["api_calls"],
        "last_repo": stats["last_repo"],
        "errors": stats["errors"],
        "rate_limits_header1": stats.get("rate_limits_header1"),
        "rate_limits_header2": stats.get("rate_limits_header2"),
        "avg_repos_per_hour": (
            stats["repos_processed"] / (runtime / 3600) if runtime > 0 else 0
        ),
        "avg_api_calls_per_minute": (
            stats["api_calls"] / (runtime / 60) if runtime > 0 else 0
        ),
        "avg_pages_fetched_per_minute": (
            stats["pages_fetched"] / (runtime / 60) if runtime > 0 else 0
        ),
        "stargazers_fetched": stats["stargazers_fetched"],
        "contributors_fetched": stats["contributors_fetched"],
        "forks_fetched": stats["forks_fetched"],
        "issues_fetched": stats["issues_fetched"],
        "users_processed": stats["users_processed"],
        "last_user_processing": stats["last_user_processing"],
        "all_users": stats["all_users"],
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
        id_repo INTEGER PRIMARY KEY,
        repo TEXT,
        id_owner TEXT,
        is_fork INTEGER,
        stars INTEGER,
        forks INTEGER,
        watchers INTEGER,
        open_issues INTEGER,
        has_issues BOOLEAN,
        has_projects BOOLEAN,
        has_wiki BOOLEAN,
        has_pages BOOLEAN,
        has_downloads BOOLEAN,
        created_at INTEGER,
        updated_at INTEGER,
        topics TEXT,
        processed INTEGER DEFAULT 0
    )

    """
    )

    ## delete users table
    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS users (
        id_user INTEGER PRIMARY KEY,
        user TEXT,
        processed INTEGER DEFAULT 0,
        stars_count INTEGER DEFAULT 0
    )
    """
    )

    conn.commit()

    conn.execute(
        """
    CREATE TABLE IF NOT EXISTS stargazers (
        id_repo INTEGER,
        id_user INTEGER,
        timestamp INTEGER,
        PRIMARY KEY (id_repo, id_user)
    )
    """
    )
    conn.commit()

    conn.execute(
        """
    CREATE TABLE IF NOT EXISTS contributors (
        id_repo INTEGER,
        id_user INTEGER,
        commits INTEGER,
        PRIMARY KEY (id_repo, id_user)
    )
    """
    )
    conn.commit()

    conn.execute(
        """
    CREATE TABLE IF NOT EXISTS forks (
        id_repo INTEGER,
        id_user INTEGER,
        timestamp INTEGER,
        id_fork INTEGER,
        PRIMARY KEY (id_fork)
    )
    """
    )

    conn.commit()

    conn.execute(
        """
    CREATE TABLE IF NOT EXISTS issues (
        id_repo INTEGER,
        id_user INTEGER,
        timestamp INTEGER,
        PRIMARY KEY (id_repo, id_user, timestamp)
    )
    """
    )
    conn.commit()

    conn.close()


def add_new_users(users: List[Tuple[int, str]]):
    """Añade nuevos usuarios a la base de datos"""
    if not users:
        return

    conn = get_db_conn()
    cur = conn.cursor()
    cur.executemany(
        "INSERT OR IGNORE INTO users(id_user, user, processed, stars_count) VALUES (?, ?, 0, 0)",
        users,
    )
    conn.commit()
    conn.close()


def get_unprocessed_users_db(limit=MAX_USERS_TO_PROCESS):
    """Obtiene usuarios no procesados de la base de datos"""
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT  id_user FROM users WHERE processed=0 LIMIT ?", (limit,))
    users = [(r[0]) for r in cur.fetchall()]
    conn.close()
    return users


def add_new_repos(repos: List[dict]):
    """Añade nuevos repositorios a la base de datos"""
    if not repos:
        return

    conn = get_db_conn()
    cur = conn.cursor()
    cur.executemany(
        """INSERT OR IGNORE INTO repos(id_repo, repo, owner, processed, created_at, updated_at)
           VALUES (?, ?, ?, 0, ?, ?)""",
        [
            (r["id_repo"], r["repo"], r["owner"], r["created_at"], r["updated_at"])
            for r in repos
        ],
    )
    conn.commit()
    conn.close()


def mark_repo_as_processed_db(repo_key: int):
    """Marca un repositorio como procesado en la base de datos"""
    conn = get_db_conn()
    conn.execute("UPDATE repos SET processed=1 WHERE id_repo=?", (repo_key,))
    conn.commit()
    conn.close()


def mark_repo_as_error_db(repo_key: int):
    """Marca un repositorio como procesado en la base de datos"""
    conn = get_db_conn()
    conn.execute("UPDATE repos SET processed=-1 WHERE id_repo=?", (repo_key,))
    conn.commit()
    conn.close()


def get_unprocessed_repos_db(limit=100):
    """Obtiene repositorios no procesados de la base de datos"""
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT repo, id_repo, id_owner FROM repos WHERE processed=0 LIMIT ?", (limit,)
    )
    repos = cur.fetchall()
    conn.close()
    repos = [(r[0], r[1], r[2]) for r in repos]
    return repos


def add_user_stars_count(id_user: int, stars_count: int):
    """Actualiza el conteo de estrellas de un usuario"""
    conn = get_db_conn()
    conn.execute(
        "UPDATE users SET stars_count=? WHERE id_user=?", (stars_count, id_user)
    )
    conn.commit()
    conn.close()


def update_processed_users(users: List[int]):
    """Marca usuarios como procesados"""
    if not users:
        return

    conn = get_db_conn()
    cur = conn.cursor()
    cur.executemany(
        "UPDATE users SET processed=1 WHERE id_user=?", [(u,) for u in users]
    )

    conn.commit()
    conn.close()


def update_user_name(id_user: int, user_name: str):
    """Actualiza el nombre de usuario en la base de datos"""
    conn = get_db_conn()
    conn.execute("UPDATE users SET user=? WHERE id_user=?", (user_name, id_user))
    conn.commit()
    conn.close()


def update_users_from_contributors_db():
    """Añade nuevos usuarios desde la tabla de contributors"""
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR IGNORE INTO users(id_user, user, processed, stars_count)
        SELECT DISTINCT id_user, NULL, 0, 0 FROM contributors
        WHERE id_user NOT IN (SELECT id_user FROM users)
        """
    )
    new_users_count = cur.rowcount
    conn.commit()
    conn.close()
    if new_users_count > 0:
        logging.info(f"🆕 Agregados {new_users_count} nuevos usuarios desde stargazers")


def is_repo_processed(id_repo: int) -> bool:
    """Verifica si un repositorio ya ha sido procesado"""
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT processed FROM repos WHERE id_repo=?", (id_repo,))
    row = cur.fetchone()
    conn.close()
    return row is not None and row[0] == 1


def there_are_unprocessed_users() -> bool:
    """Verifica si existen usuarios no procesados"""
    conn = get_db_conn()
    cur = conn.cursor()
    cur.execute("SELECT id_user FROM users WHERE processed=0 LIMIT 1")
    row = cur.fetchone()
    conn.close()
    return row is not None


async def handle_rate_limit(response_headers: dict, header) -> bool:
    """Maneja el rate limiting de forma centralizada"""
    global headers
    global stats
    remaining = response_headers.get("X-RateLimit-Remaining")
    reset = response_headers.get("X-RateLimit-Reset")
    headers[header]["rate_limit"] = int(remaining)
    headers[header]["reset_time"] = int(reset)
    if remaining:
        stats[f"rate_limits_{header}"] = int(remaining)
        stats[f"reset_time_{header}"] = int(reset)
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


async def check_rate_limit():
    global stats
    global headers
    header_max_rate_limit = max(headers, key=lambda k: headers[k]["rate_limit"] or 0)
    header_min_reset_time = min(
        headers, key=lambda k: headers[k]["reset_time"] or float("inf")
    )
    logging.info(
        f" Headers max rate limit: {header_max_rate_limit}, min reset time: {header_min_reset_time}"
    )
    remaining = headers[header_max_rate_limit]["rate_limit"]
    reset_time = headers[header_min_reset_time]["reset_time"]
    if remaining <= 10 and reset_time:
        sleep_time = max(reset_time - int(time.time()), 60)
        logging.warning(f"💤 Rate limit bajo ({remaining}). Durmiendo {sleep_time}s...")
        await asyncio.sleep(sleep_time)
    logging.info(f"Header elejido {header_max_rate_limit}, ratelimit: {remaining}")
    return header_max_rate_limit


def load_stats_from_db():
    """Carga estadísticas desde la base de datos al iniciar"""
    global stats
    conn = get_db_conn()
    cur = conn.cursor()

    # Cargar conteos
    cur.execute("SELECT COUNT(*) FROM repos WHERE processed=1")
    stats["all_repos_processed"] = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM users WHERE processed=1")
    stats["users_processed"] = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM repos")
    stats["repos_fetched"] = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM stargazers")
    stats["stargazers_fetched"] = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM contributors")
    stats["contributors_fetched"] = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM forks")
    stats["forks_fetched"] = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM issues")
    stats["issues_fetched"] = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM users")
    stats["all_users"] = cur.fetchone()[0]

    conn.close()
    logging.info(
        f"Estadísticas cargadas desde DB: {stats['repos_processed']} repos, {stats['users_processed']} usuarios procesados"
    )


async def github_request_async(
    session: aiohttp.ClientSession,
    url: str,
    params: Optional[Dict] = None,
    retries: int = 3,
) -> Optional[Dict]:
    """Versión asíncrona de github_request con manejo mejorado de rate limiting"""
    global stats
    global headers
    async with request_semaphore:
        stats["api_calls"] += 1

        for attempt in range(retries):
            try:
                h = await check_rate_limit()
                async with session.get(
                    url, headers=headers[h]["header"], params=params
                ) as resp:
                    # Manejar rate limiting
                    rate_limited = await handle_rate_limit(resp.headers, h)
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


MAX_RETRIES = 5


async def get_paginated_async(
    session: aiohttp.ClientSession,
    url: str,
    params: Optional[Dict] = {},
    max_items: Optional[int] = None,
) -> AsyncGenerator[List[Dict], None]:
    """Generador asíncrono para paginación mejorado"""
    global headers
    current_url = url
    items_fetched = 0
    page = 1

    while current_url and (max_items is None or items_fetched < max_items):
        page_params = params.copy()
        page_params["per_page"] = 100

        # Reintentos para la página actual
        for attempt in range(MAX_RETRIES):
            try:
                async with request_semaphore:
                    h = await check_rate_limit()
                    async with session.get(
                        current_url, headers=headers[h]["header"], params=page_params
                    ) as resp:
                        stats["api_calls"] += 1
                        text = await resp.text()
                batch = orjson.loads(text)
                # Limitar items si se especifica max_items
                if max_items and items_fetched + len(batch) > max_items:
                    batch = batch[: max_items - items_fetched]

                items_fetched += len(batch)
                yield batch

                if not batch or (max_items and items_fetched >= max_items):
                    return  # Terminar completamente

                # Usar Link header para siguiente página
                links = resp.headers.get("Link", "")
                current_url = None
                for part in links.split(","):
                    if 'rel="next"' in part:
                        current_url = part[part.find("<") + 1 : part.find(">")]

                page += 1
                stats["pages_fetched"] += 1
                logging.info(f"➡️ Página {page} obtenida, total items: {items_fetched}")

                # Seguridad: evitar bucles infinitos
                if page > 5000:
                    logging.warning(f"⚠️ Alcanzado límite de páginas, deteniendo...")
                    return
                rate_limited = await handle_rate_limit(resp.headers, h)
                if rate_limited:
                    continue
                break

            except Exception as e:
                if attempt == MAX_RETRIES - 1:  # Último intento
                    logging.error(
                        f"❌ Pagina {page} fallo despues de {MAX_RETRIES} intentos:"
                    )
                    raise
                await asyncio.sleep(2**attempt)  # 1s, 2s, 4s


def save_data_batch(data_list: List[Dict], table_name: str, columns=None):
    """
    Guarda datos en SQLite de forma eficiente en memoria.
    - data_list: lista de diccionarios
    - table_name: nombre de la tabla
    - columns: lista opcional de columnas a guardar
    """
    if not data_list:
        return

    try:
        # Conexión
        conn = get_db_conn()
        cur = conn.cursor()

        if columns is None:
            columns = list(data_list[0].keys())

        # Preparar inserción en batch
        placeholders = ", ".join(["?" for _ in columns])
        insert_sql = f"INSERT OR IGNORE INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

        # Convertir data_list a tupla de valores en el orden de columns
        values = [tuple(item.get(col, None) for col in columns) for item in data_list]
        print(f"Guardando {len(values)} registros en {table_name}... {insert_sql}")

        cur.executemany(insert_sql, values)
        conn.commit()
        conn.close()

        # Limpieza
        del values
        gc.collect()

    except Exception as e:
        logging.error(
            f"❌ Error guardando datos en SQLite tabla {table_name}: {e}"
            f"{''.join(traceback.format_exception(type(e), e, e.__traceback__))}"
        )


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
    id_repo: int,
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
                            "id_repo": id_repo,
                            "id_user": item["user"]["id"],
                            "timestamp": item.get("starred_at", ""),
                        }
                    )
                elif endpoint_name == "contributors":
                    data.append(
                        {
                            "id_repo": id_repo,
                            "id_user": item["id"],
                            "commits": item.get("contributions", 0),
                        }
                    )
                elif endpoint_name == "forks":
                    data.append(
                        {
                            "id_repo": id_repo,
                            "id_user": item["owner"]["id"],
                            "timestamp": int(
                                datetime.strptime(
                                    item.get("created_at", ""), "%Y-%m-%dT%H:%M:%SZ"
                                ).timestamp()
                            ),
                            "id_fork": item["id"],
                        }
                    )
                elif endpoint_name == "issues":
                    if "pull_request" not in item:  # Filtrar PRs
                        data.append(
                            {
                                "id_repo": id_repo,
                                "id_user": item["user"]["id"],
                                "timestamp": int(
                                    datetime.strptime(
                                        item.get("created_at", ""), "%Y-%m-%dT%H:%M:%SZ"
                                    ).timestamp()
                                ),
                            }
                        )
            except KeyError as e:
                logging.error(f"❌ Error procesando item en {endpoint_name}: {e}")
                continue
            except Exception as e:
                logging.error(f"❌ Error inesperado en {endpoint_name}: {e}")
                continue

        # Procesar en lotes para evitar acumulación en memoria
        if len(data) >= BATCH_SIZE:
            save_data_batch(data, endpoint_name)
            stats[f"{endpoint_name}_fetched"] += len(data)
            data = []  # Limpiar lista

    # Guardar resto si queda algo
    if data:
        save_data_batch(data, endpoint_name)
        stats[f"{endpoint_name}_fetched"] += len(data)
    stats["repos_processed"] += 1
    stats["all_repos_processed"] += 1
    mark_repo_as_processed_db(id_repo)
    return []  # Retornamos lista vacía ya que guardamos directamente


async def get_repo_data_async(
    session: aiohttp.ClientSession,
    id_repo: int,
    id_owner: int,
    owner_name: str,
    repo: str,
) -> Tuple[Optional[Dict], List, List, List, List]:
    """Versión asíncrona optimizada de get_repo_data"""
    global stats
    repo_key = f"{owner_name}/{repo}"
    stats["last_repo"] = repo_key

    logging.info(f"🔍 Procesando {repo_key} (async)...")

    # Obtener información básica del repositorio
    repo_json = await github_request_async(
        session, f"{BASE_URL}/repos/{owner_name}/{repo}"
    )
    if not repo_json:
        mark_repo_as_error_db(id_repo)
        raise ValueError("No se pudo obtener información del repositorio")

    repo_info = {
        "id_repo": id_repo,
        "repo": repo_key,
        "id_owner": id_owner,
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
        "created_at": int(
            datetime.strptime(
                repo_json.get("created_at", ""), "%Y-%m-%dT%H:%M:%SZ"
            ).timestamp()
        ),
        "updated_at": int(
            datetime.strptime(
                repo_json.get("updated_at", ""), "%Y-%m-%dT%H:%M:%SZ"
            ).timestamp()
        ),
        "topics": ",".join(repo_json.get("topics", [])),
        "processed": False,
    }
    save_data_batch([repo_info], "repos")
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
        fetch_repo_endpoint(session, endpoint_url, id_repo, endpoint_name)
        for endpoint_url, endpoint_name in endpoints
    ]

    # Esperar a que todas las tareas se completen
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Manejar excepciones
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            endpoint_name = endpoints[i][1]
            logging.error(f"❌ Error en endpoint {endpoint_name} para {repo_key}\n")
            # f"{''.join(traceback.format_exception(type(result), result, result.__traceback__))}"

    logging.info(
        f"✅ {repo_key} completado (async) - ({stats['repos_processed']} repos totales)"
    )

    # Forzar garbage collection periódicamente
    if stats["repos_processed"] % FORCE_GC_EVERY == 0:
        gc.collect()
        logging.info(
            f"🧹 Garbage collection ejecutado (repo #{stats['repos_processed']})"
        )

    return


async def process_users_cycle_async(session: aiohttp.ClientSession):
    """Versión asíncrona del procesamiento de usuarios"""
    global stats

    logging.info("🔄 Iniciando ciclo de procesamiento de usuarios (async)...")
    # Añadir nuevos usuarios desde stargazers no procesados
    update_users_from_contributors_db()
    # Obtener usuarios únicos no procesados de la base de datos
    unprocessed_users = get_unprocessed_users_db(MAX_USERS_TO_PROCESS)

    if not unprocessed_users:
        logging.info("ℹ️ No hay usuarios pendientes para procesar")
        return

    logging.info(f"👤 Procesando {len(unprocessed_users)} usuarios...")

    # Procesar usuarios concurrentemente
    async def process_single_user(id_user: int) -> Tuple[str, List[Dict], int]:
        """Procesa un usuario individual de forma asíncrona"""
        logging.info(f"👤 Procesando usuario: {id_user}")

        new_repos = []
        starred_repos = []
        user_starred_count = 0

        try:
            # obtener información del usuario
            user_json = await github_request_async(
                session, f"{BASE_URL}/user/{id_user}"
            )
            if not user_json:
                raise ValueError("No se pudo obtener información del usuario")
            user = user_json.get("login", "")
            if not user:
                raise ValueError("Usuario sin login válido")
            update_user_name(id_user, user_json.get("login", ""))

            # Obtener repositorios estrellados por el usuario usando paginación asíncrona
            async for batch in get_paginated_async(
                session, f"{BASE_URL}/users/{user}/starred"
            ):
                if not batch:
                    continue
                logging.info(f"url: {BASE_URL}/users/{user}/starred")
                for item in batch:
                    user_starred_count += 1
                    starred_repo = item["repo"]
                    repo_key = starred_repo["full_name"]

                    new_repo_info = {
                        "id_repo": starred_repo["id"],
                        "repo": repo_key,
                        "id_owner": starred_repo["owner"]["id"],
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
                        "created_at": int(
                            datetime.strptime(
                                starred_repo.get("created_at", ""), "%Y-%m-%dT%H:%M:%SZ"
                            ).timestamp()
                        ),
                        "updated_at": int(
                            datetime.strptime(
                                starred_repo.get("updated_at", ""), "%Y-%m-%dT%H:%M:%SZ"
                            ).timestamp()
                        ),
                        "topics": ",".join(starred_repo.get("topics", [])),
                        "processed": False,
                    }

                    new_repos.append(new_repo_info)
                    starred_repos.append(
                        {
                            "id_repo": starred_repo["id"],
                            "id_user": id_user,
                            "timestamp": int(
                                datetime.strptime(
                                    item.get("starred_at", ""), "%Y-%m-%dT%H:%M:%SZ"
                                ).timestamp()
                            ),
                        }
                    )

            return id_user, new_repos, user_starred_count, starred_repos

        except Exception as e:
            logging.error(
                f"❌ Error procesando usuarios: {e}",
                f"{''.join(traceback.format_exception(type(e), e, e.__traceback__))}",
            )

    # Ejecutar procesamiento de usuarios con límite de concurrencia
    semaphore = asyncio.Semaphore(
        SEMAPHORE_USER_LIMIT
    )  # Límite más conservador para usuarios

    async def bounded_process_user(id_user: int):
        async with semaphore:
            return await process_single_user(id_user)

    user_tasks = [bounded_process_user(id_user) for id_user in unprocessed_users]
    user_results = await asyncio.gather(*user_tasks, return_exceptions=True)

    # Procesar resultados
    all_new_repos = []
    processed_users = []

    for result in user_results:
        if isinstance(result, Exception):
            logging.error(f"❌ Error procesando usuario: {result}")
            continue

        id_user, new_repos, stars_count, starred_repos = result
        all_new_repos.extend(new_repos)
        processed_users.append(id_user)

        # Actualizar stars_count para el usuario
        add_user_stars_count(id_user, stars_count)

        # Guardar repositorios estrellados en la base de datos
        if starred_repos:
            save_data_batch(starred_repos, "stargazers")
        stats["users_processed"] += 1

    # Guardar nuevos repos en la base de datos
    if all_new_repos:
        save_data_batch(all_new_repos, "repos")
        logging.info(
            f"🆕 Agregados {len(all_new_repos)} nuevos repositorios para procesar"
        )

    # Marcar usuarios como procesados
    if processed_users:
        update_processed_users(processed_users)
        logging.info(f"✅ Marcados {len(processed_users)} usuarios como procesados")

    stats["last_user_processing"] = time.time()
    logging.info("🔄 Ciclo de procesamiento de usuarios completado (async)")


async def process_repos_concurrently(
    session: aiohttp.ClientSession,
    repos_to_process: List[Tuple[str, int, int]],
):
    """Procesa múltiples repositorios concurrentemente"""

    async def process_single_repo(repo_full_name: str, id_repo: int, id_owner: int):
        repo_parts = repo_full_name.split("/")
        owner = repo_parts[0]
        name = repo_parts[1]
        if is_repo_processed(repo_full_name):
            logging.info(f"ℹ️ {repo_full_name} ya procesado, saltando...")
            return True
        try:
            await get_repo_data_async(session, id_repo, id_owner, owner, name)
            update_status()
            return True
        except Exception as e:
            logging.error(f"❌ Error procesando repo {repo_full_name}: {e}")
            return False

    # Procesar repos en lotes concurrentes
    semaphore_repo = asyncio.Semaphore(SEMAPHORE_REPO_LIMIT)

    async def bounded_process(name: str, id_repo: int, id_owner: int):
        async with semaphore_repo:
            return await process_single_repo(name, id_repo, id_owner)

    tasks = [
        bounded_process(repo, id_repo, id_owner)
        for (repo, id_repo, id_owner) in repos_to_process
    ]
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
    update_users_from_contributors_db()
    load_stats_from_db()
    update_status()
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    last_progress_log = time.time()

    # Configurar sesión HTTP asíncrona con timeouts más generosos
    timeout = aiohttp.ClientTimeout(
        total=60,  # request completa
        connect=10,  # handshake TCP
        sock_read=30,  # tiempo máximo entre chunks de datos
    )
    connector = aiohttp.TCPConnector(
        limit=10,
        ttl_dns_cache=300,
        use_dns_cache=True,
        ssl=False,  # Deshabilitar SSL para evitar problemas
    )

    try:
        async with aiohttp.ClientSession(
            timeout=timeout, connector=connector
        ) as session:

            while True:
                try:
                    # Verificar si es tiempo de crear backup
                    if time.time() - stats["last_backup"] > BACKUP_TIMEOUT:
                        backup_data_folder()

                    # Verificar si es tiempo de procesar usuarios
                    b = there_are_unprocessed_users()
                    if b:
                        await process_users_cycle_async(session)

                    # Procesar repositorios no procesados primero (usar BD si está disponible)
                    unprocessed_repos = get_unprocessed_repos_db(
                        UNPROCESSED_REPOS_TO_PROCESS
                    )

                    if unprocessed_repos:
                        logging.info(
                            f"🔄 Procesando {len(unprocessed_repos)} repositorios pendientes concurrentemente..."
                        )

                        # Procesar en lotes pequeños para no sobrecargar
                        batch_size = min(
                            REPOS_BATCH_SIZE, len(unprocessed_repos)
                        )  # Lotes más pequeños
                        for i in range(0, len(unprocessed_repos), batch_size):
                            batch = unprocessed_repos[i : i + batch_size]
                            logging.info(f"🚀 Procesando repositorios: {batch}")
                            update_status()
                            await process_repos_concurrently(session, batch)
                            if time.time() - last_progress_log > 300:
                                log_progress()
                                last_progress_log = time.time()

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
                        new_repos.append((name, r["id"], r["owner"]["id"]))
                        r["repo"] = name
                        r["owner"] = owner
                        r["created_at"] = r.get("created_at", "")
                        r["updated_at"] = r.get("updated_at", "")

                    # Procesar nuevos repos concurrentemente
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
                    logging.error(f"💥 Error en loop principal: {e}", exc_info=True)
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


async def shutdown(signal, loop):
    """Cleanup resources and shutdown gracefully"""
    logging.info(f"Received exit signal {signal.name}...")

    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]

    for task in tasks:
        task.cancel()

    logging.info(f"Cancelling {len(tasks)} outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()
    update_status()
    log_progress()
    logging.info("🏁 Shutdown complete")


def handle_exception(loop, context):
    """Handle exceptions in the event loop"""
    msg = context.get("exception", context["message"])
    logging.error(f"Caught exception: {msg}")
    logging.info("Shutting down...")
    asyncio.create_task(shutdown(signal.SIGTERM, loop))


if __name__ == "__main__":
    # Configurar el manejo de señales
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    signals = (signal.SIGTERM, signal.SIGINT)

    for s in signals:
        loop.add_signal_handler(s, lambda s=s: asyncio.create_task(shutdown(s, loop)))

    # Configurar el manejador de excepciones
    loop.set_exception_handler(handle_exception)

    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logging.info("🛑 Interrupción recibida, iniciando cierre seguro...")
    except Exception as e:
        logging.error(f"💥 Error crítico: {e}")
        raise
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
