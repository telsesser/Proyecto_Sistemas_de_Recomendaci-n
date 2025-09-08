#!/usr/bin/env python3
"""
Script para monitorear el progreso del scraper de GitHub
Uso: python monitor.py [--watch]
"""

import json
import os
import time
import argparse
from datetime import datetime, timedelta


def load_status():
    """Carga el archivo de estado"""
    status_file = "data/status.json"
    if not os.path.exists(status_file):
        return None

    try:
        with open(status_file, "r") as f:
            return json.load(f)
    except Exception as e:
        print(f"❌ Error leyendo status: {e}")
        return None


def format_duration(seconds):
    """Formatea duración en formato legible"""
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        return f"{seconds//60:.0f}m {seconds%60:.0f}s"
    else:
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        return f"{hours:.0f}h {minutes:.0f}m"


def check_log_size():
    """Verifica el tamaño del archivo de log"""
    log_file = "data/api_consumer.log"
    if os.path.exists(log_file):
        size_mb = os.path.getsize(log_file) / 1024 / 1024
        return size_mb
    return 0


def get_recent_errors():
    """Obtiene errores recientes del log"""
    log_file = "data/api_consumer.log"
    if not os.path.exists(log_file):
        return []

    errors = []
    try:
        with open(log_file, "r", encoding="utf-8") as f:
            lines = f.readlines()
            # Obtener últimas 50 líneas
            for line in lines[-50:]:
                if "[ERROR]" in line or "❌" in line:
                    errors.append(line.strip())
    except Exception:
        pass

    return errors[-5:]  # Últimos 5 errores


def display_status(status_data):
    """Muestra el estado actual"""
    if not status_data:
        print("❌ No se puede leer el archivo de estado")
        return

    print("=" * 60)
    print("📊 ESTADO DEL SCRAPER DE GITHUB")
    print("=" * 60)

    # Información básica
    timestamp = datetime.fromisoformat(status_data["timestamp"])
    now = datetime.now()
    time_since_update = (now - timestamp).total_seconds()

    print(f"🕒 Última actualización: {timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"⏰ Hace: {format_duration(time_since_update)}")

    if time_since_update > 600:  # 10 minutos
        print("⚠️  WARNING: Hace más de 10 minutos sin actualización")

    print()

    # Estadísticas principales
    print("📈 ESTADÍSTICAS:")
    print(f"   Repos procesados: {status_data['repos_processed']:,}")
    print(f"   Páginas procesadas: {status_data['pages_processed']:,}")
    print(f"   Llamadas API: {status_data['api_calls']:,}")
    print(f"   Errores: {status_data['errors']:,}")
    print(f"   Rate limits: {status_data['rate_limits']:,}")
    print(f"   Último repo: {status_data['last_repo']}")
    print()

    # Tiempo y rendimiento
    print("⏱️ RENDIMIENTO:")
    print(f"   Tiempo corriendo: {status_data['runtime_formatted']}")
    print(f"   Repos/hora: {status_data['avg_repos_per_hour']:.1f}")
    print(f"   API calls/min: {status_data['avg_api_calls_per_minute']:.1f}")
    print()

    # Estado del log
    log_size = check_log_size()
    print(f"📝 Log: {log_size:.1f} MB")
    if log_size > 100:
        print("⚠️  WARNING: Log muy grande, considera rotarlo")
    print()

    # Errores recientes
    recent_errors = get_recent_errors()
    if recent_errors:
        print("🚨 ERRORES RECIENTES:")
        for error in recent_errors:
            print(f"   {error}")
        print()

    # Estimaciones
    if status_data["avg_repos_per_hour"] > 0:
        # Estimación muy aproximada basada en búsqueda de GitHub
        estimated_total = 100000  # Estimación conservadora de repos Python
        remaining = max(0, estimated_total - status_data["repos_processed"])
        if remaining > 0 and status_data["avg_repos_per_hour"] > 0:
            eta_hours = remaining / status_data["avg_repos_per_hour"]
            eta = now + timedelta(hours=eta_hours)
            print("🎯 ESTIMACIONES:")
            print(f"   Repos restantes: ~{remaining:,}")
            print(f"   ETA: ~{eta.strftime('%Y-%m-%d %H:%M')}")
            print(f"   Tiempo restante: ~{format_duration(eta_hours * 3600)}")

    print("=" * 60)


def watch_mode():
    """Modo watch - actualiza cada 30 segundos"""
    print("👁️  Modo watch activado (Ctrl+C para salir)")
    print()

    try:
        while True:
            os.system("clear" if os.name == "posix" else "cls")
            status_data = load_status()
            display_status(status_data)
            print("\n🔄 Actualizando en 30 segundos...")
            time.sleep(30)
    except KeyboardInterrupt:
        print("\n👋 Saliendo del modo watch")


def main():
    parser = argparse.ArgumentParser(description="Monitor del scraper de GitHub")
    parser.add_argument(
        "--watch", action="store_true", help="Modo watch - actualiza cada 30 segundos"
    )

    args = parser.parse_args()

    if args.watch:
        watch_mode()
    else:
        status_data = load_status()
        display_status(status_data)


if __name__ == "__main__":
    main()
