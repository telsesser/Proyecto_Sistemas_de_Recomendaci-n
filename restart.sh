#!/bin/bash

# Configuración
SCRIPT_NAME="tu_script.py"  # Cambia por el nombre de tu script
LOG_FILE="output.log"

echo "🔄 Reiniciando $SCRIPT_NAME..."

# 1. Buscar y terminar proceso existente
PID=$(pgrep -f "$SCRIPT_NAME")
if [ ! -z "$PID" ]; then
    echo "📍 Proceso encontrado con PID: $PID"
    echo "🛑 Terminando proceso..."
    kill $PID
    
    # Esperar hasta 10 segundos para terminación graceful
    for i in {1..10}; do
        if ! kill -0 $PID 2>/dev/null; then
            echo "✅ Proceso terminado gracefully"
            break
        fi
        echo "⏳ Esperando terminación... ($i/10)"
        sleep 1
    done
    
    # Si aún existe, forzar terminación
    if kill -0 $PID 2>/dev/null; then
        echo "⚠️ Forzando terminación..."
        kill -9 $PID
        sleep 2
    fi
else
    echo "ℹ️ No se encontró proceso en ejecución"
fi

# 2. Verificar que no hay procesos restantes
REMAINING=$(pgrep -f "$SCRIPT_NAME")
if [ ! -z "$REMAINING" ]; then
    echo "❌ Error: Aún hay procesos corriendo"
    exit 1
fi

# 3. Hacer backup del log anterior si existe
if [ -f "$LOG_FILE" ]; then
    TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
    mv "$LOG_FILE" "${LOG_FILE}.${TIMESTAMP}"
    echo "📄 Log anterior guardado como ${LOG_FILE}.${TIMESTAMP}"
fi

# 4. Reiniciar el script
echo "🚀 Iniciando $SCRIPT_NAME..."
nohup python "$SCRIPT_NAME" > "$LOG_FILE" 2>&1 &
NEW_PID=$!

# 5. Verificar que se inició correctamente
sleep 3
if kill -0 $NEW_PID 2>/dev/null; then
    echo "✅ Script reiniciado exitosamente con PID: $NEW_PID"
    echo "📝 Logs en: $LOG_FILE"
    echo "👁️ Para monitorear: tail -f $LOG_FILE"
    echo "📊 Para ver estado: python monitor.py"
else
    echo "❌ Error: El script no se pudo iniciar"
    exit 1
fi