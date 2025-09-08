#!/usr/bin/env python3
"""
Monitor simple para el scraper de GitHub
Uso: python monitor.py [--watch] [--interval 5]
"""

import json
import os
import time
import argparse
import sys
from datetime import datetime


def clear_screen():
    """Limpia la pantalla"""
    os.system("cls" if os.name == "nt" else "clear")


def format_bytes(bytes_val):
    """Convierte bytes a formato legible"""
    for unit in ["B", "KB", "MB", "GB"]:
        if bytes_val < 1024.0:
            return f"{bytes_val:.1f} {unit}"
        bytes_val /= 1024.0
    return f"{bytes_val:.1f} TB"


def get_file_info(filepath):
    """Obtiene info de un archivo parquet"""
    if not os.path.exists(filepath):
        return {"exists": False, "size": 0, "size_str": "0 B", "modified": "N/A"}

    stat = os.stat(filepath)
    size = stat.st_size
    modified = datetime.fromtimestamp(stat.st_mtime).strftime("%H:%M:%S")

    return {
        "exists": True,
        "size": size,
        "size_str": format_bytes(size),
        "modified": modified,
    }


def show_status():
    """Muestra el estado actual del scraper"""
    data_dir = "data"
    status_file = os.path.join(data_dir, "status.json")
    log_file = os.path.join(data_dir, "api_consumer.log")

    # Leer status.json
    if os.path.exists(status_file):
        try:
            with open(status_file, "r") as f:
                status = json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            status = {"error": "No se pudo leer status.json"}
    else:
        status = {"error": "status.json no encontrado"}

    # Info de archivos
    files = {
        "repos": get_file_info(os.path.join(data_dir, "repos.parquet")),
        "stars": get_file_info(os.path.join(data_dir, "stars.parquet")),
        "contributors": get_file_info(os.path.join(data_dir, "contributors.parquet")),
        "forks": get_file_info(os.path.join(data_dir, "forks.parquet")),
        "issues": get_file_info(os.path.join(data_dir, "issues.parquet")),
    }

    # Últimas líneas del log
    log_lines = []
    if os.path.exists(log_file):
        try:
            with open(log_file, "r", encoding="utf-8") as f:
                log_lines = f.readlines()[-5:]  # Últimas 5 líneas
        except:
            log_lines = ["Error leyendo log"]

    print("=" * 80)
    print("🔍 MONITOR GITHUB SCRAPER")
    print("=" * 80)
    print(f"⏰ Actualizado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    if "error" in status:
        print(f"❌ ERROR: {status['error']}")
        print()
    else:
        # Estado general
        print("📊 ESTADÍSTICAS GENERALES")
        print("-" * 40)
        print(f"🏃 Runtime:           {status.get('runtime_formatted', 'N/A')}")
        print(f"📦 Repos procesados:  {status.get('repos_processed', 0):,}")
        print(f"📄 Páginas:           {status.get('pages_processed', 0):,}")
        print(f"🌐 API calls:         {status.get('api_calls', 0):,}")
        print(f"❌ Errores:           {status.get('errors', 0):,}")
        print(f"⏸️ Rate limits:       {status.get('rate_limits', 0)}")
        print(f"📈 Repos/hora:        {status.get('avg_repos_per_hour', 0):.1f}")
        print(f"📈 Calls/minuto:      {status.get('avg_api_calls_per_minute', 0):.1f}")
        print(f"🎯 Último repo:       {status.get('last_repo', 'N/A')}")
        print()

        # Datos recolectados
        print("🗃️ DATOS RECOLECTADOS")
        print("-" * 40)
        print(f"⭐ Stars:            {status.get('stars_fetched', 0):,}")
        print(f"👥 Contributors:     {status.get('contributors_fetched', 0):,}")
        print(f"🍴 Forks:            {status.get('forks_fetched', 0):,}")
        print(f"🐞 Issues:           {status.get('issues_fetched', 0):,}")
        print()

    # Archivos
    print("📁 ARCHIVOS PARQUET")
    print("-" * 40)
    total_size = 0
    for name, info in files.items():
        if info["exists"]:
            print(f"{name:12} │ {info['size_str']:>8} │ {info['modified']}")
            total_size += info["size"]
        else:
            print(f"{name:12} │     N/A  │   N/A")
    print("-" * 40)
    print(f"{'TOTAL':12} │ {format_bytes(total_size):>8} │")
    print()

    # Log reciente
    print("📝 LOG RECIENTE (últimas 5 líneas)")
    print("-" * 60)
    if log_lines:
        for line in log_lines:
            # Limpiar y truncar líneas muy largas
            clean_line = line.strip()
            if len(clean_line) > 75:
                clean_line = clean_line[:72] + "..."
            print(clean_line)
    else:
        print("No hay logs disponibles")

    print("=" * 80)


def monitor_loop(interval):
    """Loop de monitoreo continuo"""
    try:
        while True:
            clear_screen()
            show_status()
            print(f"\n🔄 Actualizando cada {interval} segundos... (Ctrl+C para salir)")
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\n👋 Monitor detenido")
        sys.exit(0)


def main():
    parser = argparse.ArgumentParser(description="Monitor para GitHub scraper")
    parser.add_argument(
        "--watch", "-w", action="store_true", help="Modo watch continuo"
    )
    parser.add_argument(
        "--interval",
        "-i",
        type=int,
        default=5,
        help="Intervalo en segundos para --watch (default: 5)",
    )

    args = parser.parse_args()

    if args.watch:
        monitor_loop(args.interval)
    else:
        show_status()


if __name__ == "__main__":
    main()
