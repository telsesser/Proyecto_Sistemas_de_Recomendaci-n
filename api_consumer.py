import requests
import time
import os
import json
import logging
from dotenv import load_dotenv
import pandas as pd
import fastparquet


START_DATE = "2025-09-01"
PARQUET_ENGINE = "fastparquet"

# Configuración de logging
DATA_DIR = "data"
if not os.path.exists(DATA_DIR):
    os.makedirs(DATA_DIR)
LOG_PATH = os.path.join(DATA_DIR, "api_consumer.log")

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.FileHandler(LOG_PATH, encoding="utf-8"), logging.StreamHandler()],
)


# Cargar variables de entorno desde .env
load_dotenv()
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN2")

if not GITHUB_TOKEN:
    raise ValueError("❌ No se encontró GITHUB_TOKEN en el .env")

HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3.star+json",
}
BASE_URL = "https://api.github.com"


def github_request(url, params=None, retries=3):
    for attempt in range(retries):
        resp = requests.get(url, headers=HEADERS, params=params, allow_redirects=True)

        # Mostrar info de rate limit
        limit = resp.headers.get("X-RateLimit-Limit")
        remaining = resp.headers.get("X-RateLimit-Remaining")
        reset = resp.headers.get("X-RateLimit-Reset")
        if remaining is not None and reset is not None:
            reset_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(reset)))
            logging.info(
                f"RateLimit: {remaining}/{limit} requests restantes (reset: {reset_time})"
            )

        if resp.status_code == 403 and resp.headers.get("X-RateLimit-Remaining") == "0":
            reset_time = int(resp.headers.get("X-RateLimit-Reset", time.time() + 60))
            sleep_time = max(reset_time - int(time.time()), 60)
            logging.warning(f"Rate limit alcanzado. Durmiendo {sleep_time} segundos...")
            time.sleep(sleep_time)
            continue
        if resp.ok:
            return resp.json()
        time.sleep(2**attempt)
    return None


def get_paginated(url, params=None):
    results = []
    page = 1
    while url:
        if not params:
            params = {}
        params["per_page"] = 100
        logging.debug(f"[get_paginated] Página {page} - URL: {url}")
        resp = requests.get(url, headers=HEADERS, params=params)

        # Mostrar info de rate limit
        limit = resp.headers.get("X-RateLimit-Limit")
        remaining = resp.headers.get("X-RateLimit-Remaining")
        reset = resp.headers.get("X-RateLimit-Reset")
        if remaining is not None and reset is not None:
            reset_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(reset)))
            logging.debug(
                f"RateLimit: {remaining}/{limit} requests restantes (reset: {reset_time})"
            )
        if not resp.ok:
            logging.error(
                f"[get_paginated] Error en request: status {resp.status_code}"
            )
            break
        batch = resp.json()
        logging.debug(
            f"[get_paginated] Resultados obtenidos en página {page}: {len(batch)}"
        )
        results.extend(batch)

        # link header
        links = resp.headers.get("Link", "")
        next_url = None
        for part in links.split(","):
            if 'rel="next"' in part:
                next_url = part[part.find("<") + 1 : part.find(">")]
        url = next_url
        page += 1
    logging.debug(f"[get_paginated] Resultados totales obtenidos: {len(results)}")
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
    repo_json = github_request(f"{BASE_URL}/repos/{owner}/{repo}")

    repo_info = {
        "repo": f"{owner}/{repo}",
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
    logging.info(f"⭐ Descargando stargazers de {owner}/{repo}...")
    stargazers = get_paginated(f"{BASE_URL}/repos/{owner}/{repo}/stargazers")
    logging.info(f"→ {len(stargazers)} stargazers encontrados")
    for s in stargazers:
        stars.append(
            {
                "repo": f"{owner}/{repo}",
                "user": s["user"]["login"],
                "timestamp": s["starred_at"],
            }
        )

    # Contributors
    logging.info(f"👥 Descargando contributors de {owner}/{repo}...")
    contribs = get_paginated(f"{BASE_URL}/repos/{owner}/{repo}/contributors")
    logging.info(f"→ {len(contribs)} contributors encontrados")
    for c in contribs:
        contributors.append(
            {
                "repo": f"{owner}/{repo}",
                "user": c["login"],
                "commits": c["contributions"],
            }
        )

    # Forks
    logging.info(f"🍴 Descargando forks de {owner}/{repo}...")
    repo_forks = get_paginated(f"{BASE_URL}/repos/{owner}/{repo}/forks")
    logging.info(f"→ {len(repo_forks)} forks encontrados")
    for f in repo_forks:
        forks.append(
            {
                "repo": f"{owner}/{repo}",
                "user": f["owner"]["login"],
                "timestamp": f["created_at"],
                "name": f["full_name"],
                "url": f["html_url"],
            }
        )

    # Issues
    logging.info(f"🐞 Descargando issues de {owner}/{repo}...")
    repo_issues = get_paginated(
        f"{BASE_URL}/repos/{owner}/{repo}/issues", params={"state": "all"}
    )
    logging.info(f"→ {len(repo_issues)} issues encontrados (incluye PRs)")
    for i in repo_issues:
        if "pull_request" not in i:
            issues.append(
                {
                    "repo": f"{owner}/{repo}",
                    "user": i["user"]["login"],
                    "timestamp": i["created_at"],
                    "url": i["html_url"],
                }
            )

    logging.info(f"✅ Finalizado scraping de {owner}/{repo}")
    return repo_info, stars, contributors, forks, issues


if __name__ == "__main__":
    # Crear carpeta /data si no existe
    DATA_DIR = "data"
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    # Cargar repos ya existentes si hay parquet
    repos_parquet_path = os.path.join(DATA_DIR, "repos.parquet")
    stars_parquet_path = os.path.join(DATA_DIR, "stars.parquet")
    contribs_parquet_path = os.path.join(DATA_DIR, "contributors.parquet")
    forks_parquet_path = os.path.join(DATA_DIR, "forks.parquet")
    issues_parquet_path = os.path.join(DATA_DIR, "issues.parquet")
    repos_raw_path = os.path.join(DATA_DIR, "repos_raw.json")

    if os.path.exists(repos_parquet_path):
        existing_repos = pd.read_parquet(repos_parquet_path)
        existing_set = set(existing_repos["repo"])
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

    page = 1
    while True:
        repos = get_repos_by_topic("python", per_page=10, page=page)
        page += 1
        stars_list = []
        contribs_list = []
        forks_list = []
        issues_list = []

        for r in repos:
            owner, name = r["owner"]["login"], r["name"]
            repo_key = f"{owner}/{name}"

            if repo_key in existing_set:
                logging.info(f"⏭️ {repo_key} ya existe, lo salto")
                continue

            logging.info(f"📂 Descargando {repo_key}...")
            repo_info, stars, contribs, forks, issues = get_repo_data(owner, name)

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
