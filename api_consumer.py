import aiohttp
import asyncio
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
import shutil
import duckdb
from typing import List, Dict, Optional, Tuple, AsyncGenerator

load_dotenv()

DEBUG = os.getenv("DEBUG", "False").lower() in ("true", "1", "t")
START_DATE = os.getenv("START_DATE", "2025-01-01")
PARQUET_ENGINE = "fastparquet"

# Configuración para prevenir memory leaks
MAX_ITEMS_PER_ENDPOINT = 3000  # Límite máximo por endpoint
BATCH_SIZE = 100  # Procesar en lotes
FORCE_GC_EVERY = 3  # Forzar garbage collection cada N repos

# NUEVA CONFIGURACIÓN: Tiempo para procesar usuarios (en segundos)
USER_PROCESSING_INTERVAL = 360
MAX_USERS_TO_PROCESS = 10  # Máximo usuarios a procesar por ciclo

# NUEVA CONFIGURACIÓN: Tiempo de espera para copias de seguridad (en segundos)
BACKUP_TIMEOUT = 3600  # 1 hora

# CONFIGURACIÓN ASÍNCRONA
MAX_CONCURRENT_REQUESTS = 10  # Máximo requests concurrentes
SEMAPHORE_LIMIT = 5  # Límite del semáforo para controlar concurrencia
REQUEST_DELAY = 0.1  # Delay entre requests para evitar rate limiting

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
        "stars_fetched": 0,
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
    logging.getLogger().setLevel(logging.INFO)
    logging.info("Modo DEBUG activado")

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN2")
if not GITHUB_TOKEN:
    raise ValueError("❌ No se encontró GITHUB_TOKEN en el .env")

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
        "stars_fetched": stats["stars_fetched"],
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


async def github_request_async(
    session: aiohttp.ClientSession,
    url: str,
    params: Optional[Dict] = None,
    retries: int = 3,
) -> Optional[Dict]:
    """Versión asíncrona de github_request con manejo de rate limiting"""
    global stats

    async with request_semaphore:
        stats["api_calls"] += 1

        for attempt in range(retries):
            try:
                await asyncio.sleep(
                    REQUEST_DELAY
                )  # Pequeño delay para evitar rate limiting

                async with session.get(url, params=params) as resp:
                    # Obtener headers de rate limiting
                    remaining = resp.headers.get("X-RateLimit-Remaining")
                    reset = resp.headers.get("X-RateLimit-Reset")

                    if remaining:
                        stats["rate_limits"] = int(remaining)

                        # Si quedan pocos requests, esperar
                        if int(remaining) <= 10:
                            reset_time = int(reset) if reset else time.time() + 60
                            sleep_time = max(reset_time - int(time.time()), 60)
                            logging.warning(
                                f"💤 Rate limit bajo ({remaining}). Durmiendo {sleep_time}s..."
                            )
                            update_status()
                            await asyncio.sleep(sleep_time)
                            continue

                    if resp.status == 200:
                        return await resp.json()
                    elif (
                        resp.status == 403
                        and "rate limit" in (await resp.text()).lower()
                    ):
                        # Rate limit específico
                        reset_time = int(reset) if reset else time.time() + 60
                        sleep_time = max(reset_time - int(time.time()), 60)
                        logging.warning(
                            f"💤 Rate limit alcanzado. Durmiendo {sleep_time}s..."
                        )
                        await asyncio.sleep(sleep_time)
                        continue
                    else:
                        stats["errors"] += 1
                        logging.error(f"❌ Error en API: {resp.status} - {url}")

            except Exception as e:
                logging.error(f"❌ Error en request: {e}")
                stats["errors"] += 1

            # Esperar antes del siguiente intento
            await asyncio.sleep(2**attempt)

        return None


async def get_paginated_async(
    session: aiohttp.ClientSession,
    url: str,
    params: Optional[Dict] = None,
    max_items: Optional[int] = None,
) -> AsyncGenerator[List[Dict], None]:
    """Generador asíncrono para paginación"""
    page = 1
    items_fetched = 0

    while url and (max_items is None or items_fetched < max_items):
        if not params:
            params = {}
        params["per_page"] = 100

        logging.info(f"[get_paginated_async] Página {page} - URL: {url}")

        # Hacer request asíncrono
        async with session.get(url, params=params) as resp:
            stats["api_calls"] += 1

            # Control de rate limiting
            remaining = resp.headers.get("X-RateLimit-Remaining")
            reset = resp.headers.get("X-RateLimit-Reset")

            if remaining:
                stats["rate_limits"] = int(remaining)

                if int(remaining) <= 10:
                    reset_time = int(reset) if reset else time.time() + 60
                    sleep_time = max(reset_time - int(time.time()), 60)
                    logging.warning(
                        f"💤 Rate limit en paginación. Durmiendo {sleep_time}s..."
                    )
                    update_status()
                    await asyncio.sleep(sleep_time)
                    continue

            if resp.status != 200:
                logging.error(f"❌ Error en paginación: status {resp.status}")
                stats["errors"] += 1
                break

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
    return data["items"]


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
            if endpoint_name == "stargazers":
                data.append(
                    {
                        "repo": repo_key,
                        "user": item["user"]["login"],
                        "timestamp": item["starred_at"],
                    }
                )
            elif endpoint_name == "contributors":
                data.append(
                    {
                        "repo": repo_key,
                        "user": item["login"],
                        "commits": item["contributions"],
                    }
                )
            elif endpoint_name == "forks":
                data.append(
                    {
                        "repo": repo_key,
                        "user": item["owner"]["login"],
                        "timestamp": item["created_at"],
                        "name": item["full_name"],
                        "url": item["html_url"],
                    }
                )
            elif endpoint_name == "issues":
                if "pull_request" not in item:  # Filtrar PRs
                    data.append(
                        {
                            "repo": repo_key,
                            "user": item["user"]["login"],
                            "timestamp": item["created_at"],
                            "url": item["html_url"],
                        }
                    )

        # Procesar en lotes para evitar acumulación en memoria
        if len(data) >= BATCH_SIZE:
            save_data_batch(data, os.path.join(DATA_DIR, f"{endpoint_name}.parquet"))
            stats[f"{endpoint_name}_fetched"] += len(data)
            data = []  # Limpiar lista

    # Guardar resto si queda algo
    if data:
        save_data_batch(data, os.path.join(DATA_DIR, f"{endpoint_name}.parquet"))
        stats[f"{endpoint_name}_fetched"] += len(data)

    return data


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
        (f"{BASE_URL}/repos/{owner}/{repo}/stargazers", "stargazers"),
        (f"{BASE_URL}/repos/{owner}/{repo}/contributors", "contributors"),
        (f"{BASE_URL}/repos/{owner}/{repo}/forks", "forks"),
        (f"{BASE_URL}/repos/{owner}/{repo}/issues", "issues"),
    ]

    # Ejecutar todas las tareas concurrentemente
    logging.info(
        f"🚀 Procesando {len(endpoints)} endpoints concurrentemente para {repo_key}..."
    )
    tasks = []

    for endpoint_url, endpoint_name in endpoints:
        task = fetch_repo_endpoint(session, endpoint_url, repo_key, endpoint_name)
        tasks.append(task)

    # Esperar a que todas las tareas se completen
    await asyncio.gather(*tasks, return_exceptions=True)

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

    stars_path = os.path.join(DATA_DIR, "stars.parquet")
    users_path = os.path.join(DATA_DIR, "users.parquet")
    repos_path = os.path.join(DATA_DIR, "repos.parquet")

    # 1. Obtener usuarios únicos de stars.parquet
    if not os.path.exists(stars_path):
        logging.info("⚠️ No existe stars.parquet, saltando procesamiento de usuarios")
        return

    # Usar duckdb para obtener usuarios únicos sin cargar todo el parquet en memoria
    con = duckdb.connect()
    query = f"SELECT DISTINCT user FROM '{stars_path}'"
    unique_users = con.execute(query).fetchdf()["user"].tolist()
    con.close()
    logging.info(f"👤 Encontrados {len(unique_users)} usuarios únicos en stars")

    # 2. Crear/actualizar users.parquet
    existing_users = set()
    if os.path.exists(users_path):
        users_df = pd.read_parquet(users_path, engine=PARQUET_ENGINE)
        existing_users = set(users_df["user"])
        logging.info(f"👥 Ya existen {len(existing_users)} usuarios procesados")
    else:
        users_df = pd.DataFrame(columns=["user", "processed", "stars_count"])

    # Agregar nuevos usuarios
    new_users = []
    for user in unique_users:
        if user not in existing_users:
            new_users.append({"user": user, "processed": False, "stars_count": 0})

    if new_users:
        new_users_df = pd.DataFrame(new_users)
        if len(existing_users) > 0:
            users_df = pd.concat([users_df, new_users_df], ignore_index=True)
        else:
            users_df = new_users_df
        users_df.to_parquet(users_path, index=False, engine=PARQUET_ENGINE)
        logging.info(f"➕ Agregados {len(new_users)} usuarios nuevos")

    # 3. Procesar usuarios no procesados (limitado) - CONCURRENTEMENTE
    unprocessed_users = (
        users_df[users_df["processed"] == False]["user"]
        .head(MAX_USERS_TO_PROCESS)
        .tolist()
    )
    logging.info(f"🔍 Procesando {len(unprocessed_users)} usuarios concurrentemente...")

    # Cargar repos existentes
    existing_repos = set()
    if os.path.exists(repos_path):
        repos_df = pd.read_parquet(repos_path, engine=PARQUET_ENGINE)
        existing_repos = set(repos_df["repo"])

    # Procesar usuarios concurrentemente
    async def process_single_user(user: str) -> Tuple[str, List[Dict], int]:
        """Procesa un usuario individual de forma asíncrona"""
        logging.info(f"👤 Procesando usuario: {user}")

        new_repos = []
        user_starred_count = 0

        # Obtener repositorios estrellados por el usuario usando paginación asíncrona
        async for batch in get_paginated_async(
            session, f"{BASE_URL}/users/{user}/starred"
        ):
            if not batch:
                continue
            for starred_data in batch:
                repo_data = starred_data["repo"]
                repo_key = repo_data["full_name"]
                user_starred_count += 1

                if repo_key not in existing_repos:
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
                        "processed": False,
                    }
                    new_repos.append(new_repo_info)

        return user, new_repos, user_starred_count

    # Ejecutar procesamiento de usuarios concurrentemente
    user_tasks = [process_single_user(user) for user in unprocessed_users]
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
        users_df.loc[users_df["user"] == user, "stars_count"] = stars_count
        stats["users_processed"] += 1

    # Guardar nuevos repos
    if all_new_repos:
        save_data_batch(all_new_repos, repos_path)
        logging.info(
            f"🆕 Agregados {len(all_new_repos)} nuevos repositorios para procesar"
        )

    # Marcar usuarios como procesados
    if processed_users:
        users_df.loc[users_df["user"].isin(processed_users), "processed"] = True
        users_df.to_parquet(users_path, index=False, engine=PARQUET_ENGINE)
        logging.info(f"✅ Marcados {len(processed_users)} usuarios como procesados")

    stats["last_user_processing"] = time.time()
    logging.info("🔄 Ciclo de procesamiento de usuarios completado (async)")


def should_process_users():
    """Determina si es tiempo de procesar usuarios"""
    return (time.time() - stats["last_user_processing"]) >= USER_PROCESSING_INTERVAL


def get_unprocessed_repos():
    """Obtiene repositorios no procesados"""
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
    """Marca un repositorio como procesado"""
    repos_path = os.path.join(DATA_DIR, "repos.parquet")
    if not os.path.exists(repos_path):
        return

    repos_df = pd.read_parquet(repos_path, engine=PARQUET_ENGINE)
    repos_df.loc[repos_df["repo"] == repo_key, "processed"] = True
    repos_df.to_parquet(repos_path, index=False, engine=PARQUET_ENGINE)
    del repos_df
    gc.collect()


async def process_repos_concurrently(
    session: aiohttp.ClientSession,
    repos_to_process: List[Tuple[str, str]],
    existing_set: set,
):
    """Procesa múltiples repositorios concurrentemente"""

    async def process_single_repo(owner: str, name: str):
        repo_key = f"{owner}/{name}"
        try:
            result = await get_repo_data_async(session, owner, name)
            if result[0] is not None:
                repo_info = result[0]
                save_data_batch([repo_info], os.path.join(DATA_DIR, "repos.parquet"))
                mark_repo_as_processed(repo_key)
                existing_set.add(repo_key)
            return True
        except Exception as e:
            logging.error(f"❌ Error procesando repo {repo_key}: {e}")
            return False

    # Procesar repos en lotes concurrentes
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async def bounded_process(owner: str, name: str):
        async with semaphore:
            return await process_single_repo(owner, name)

    tasks = [bounded_process(owner, name) for owner, name in repos_to_process]
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

    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    repos_parquet_path = os.path.join(DATA_DIR, "repos.parquet")

    # Cargar repos existentes
    if os.path.exists(repos_parquet_path):
        existing_repos = pd.read_parquet(repos_parquet_path)
        if "processed" not in existing_repos.columns:
            existing_repos["processed"] = True
        elif existing_repos["processed"].isnull().any():
            existing_repos["processed"] = existing_repos["processed"].fillna(True)
            existing_repos.to_parquet(
                repos_parquet_path, index=False, engine=PARQUET_ENGINE
            )
        existing_set = set(existing_repos["repo"])
        logging.info(f"📚 Cargados {len(existing_set)} repos existentes")
        del existing_repos
        gc.collect()
    else:
        existing_set = set()
        logging.info("📝 Iniciando desde cero")

    last_progress_log = time.time()

    # Configurar sesión HTTP asíncrona
    timeout = aiohttp.ClientTimeout(total=30, connect=10)
    connector = aiohttp.TCPConnector(
        limit=MAX_CONCURRENT_REQUESTS, limit_per_host=SEMAPHORE_LIMIT
    )

    try:
        async with aiohttp.ClientSession(
            headers=HEADERS, timeout=timeout, connector=connector
        ) as session:

            while True:
                # Verificar si es tiempo de crear backup
                if time.time() - stats["last_backup"] > BACKUP_TIMEOUT:
                    backup_data_folder()

                # Verificar si es tiempo de procesar usuarios
                if should_process_users():
                    await process_users_cycle_async(session)

                # Procesar repositorios no procesados primero
                unprocessed_repos = get_unprocessed_repos()
                if unprocessed_repos:
                    logging.info(
                        f"🔄 Procesando {len(unprocessed_repos)} repositorios pendientes concurrentemente..."
                    )

                    # Procesar en lotes para no sobrecargar
                    batch_size = min(MAX_CONCURRENT_REQUESTS, len(unprocessed_repos))
                    for i in range(0, len(unprocessed_repos), batch_size):
                        batch = unprocessed_repos[i : i + batch_size]
                        await process_repos_concurrently(session, batch, existing_set)

                        update_status()

                        if time.time() - last_progress_log > 300:
                            log_progress()
                            last_progress_log = time.time()

                        # Pequeña pausa entre lotes para no saturar la API
                        await asyncio.sleep(2)

                    continue  # Volver al inicio del loop para verificar más repos pendientes

                # Continuar con el flujo normal si no hay repos pendientes
                logging.info(f"📖 Procesando página {stats['pages_processed']}...")
                repos = await get_repos_by_topic_async(
                    session, "python", per_page=10, page=stats["pages_processed"]
                )

                if not repos:
                    logging.info("🏁 No hay más repositorios para procesar")
                    break

                # Filtrar repos que ya existen
                new_repos = []
                for r in repos:
                    owner, name = r["owner"]["login"], r["name"]
                    repo_key = f"{owner}/{name}"

                    if repo_key not in existing_set:
                        new_repos.append((owner, name))
                    else:
                        logging.info(f"⏭️ {repo_key} ya existe, saltando")

                # Procesar nuevos repos concurrentemente
                if new_repos:
                    logging.info(
                        f"🚀 Procesando {len(new_repos)} nuevos repositorios concurrentemente..."
                    )
                    await process_repos_concurrently(session, new_repos, existing_set)

                update_status()

                if time.time() - last_progress_log > 300:
                    log_progress()
                    last_progress_log = time.time()

                stats["pages_processed"] += 1
                await asyncio.sleep(2)  # Pausa entre páginas

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
