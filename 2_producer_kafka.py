"""
=============================================================
PIPELINE DE HURTOS EN MEDELLÍN
Paso 2: Producer Kafka — Streaming de eventos
=============================================================
Lee el CSV generado en el paso 1 y publica cada registro
como un mensaje JSON en el tópico 'hurtos-medellin' de Kafka.
Simula un flujo de reportes en tiempo real.
"""

import csv
import json
import time
import os
import random
from datetime import datetime

# ─────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────
KAFKA_BROKER  = "localhost:9092"
TOPIC         = "hurtos-medellin"
INPUT_FILE    = "data/hurtos_medellin_raw.csv"
DELAY_MIN     = 0.05   # Segundos entre mensajes (mínimo)
DELAY_MAX     = 0.15   # Segundos entre mensajes (máximo)
BATCH_SIZE    = 100    # Mensajes por lote antes de imprimir progreso

# ─────────────────────────────────────────────
# FUNCIONES
# ─────────────────────────────────────────────

def crear_producer():
    """Crea y retorna el producer de Kafka."""
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            acks="all",           # Garantía de entrega
            retries=3,
            batch_size=16384,
            linger_ms=10,         # Pequeño delay para agrupar mensajes
        )
        print(f"[PRODUCER] ✓ Conectado a Kafka: {KAFKA_BROKER}")
        return producer
    except Exception as e:
        print(f"[PRODUCER] ✗ Error conectando a Kafka: {e}")
        raise


def enriquecer_mensaje(row: dict) -> dict:
    """
    Enriquece cada registro con campos adicionales
    útiles para el análisis en tiempo real.
    """
    ahora = datetime.now().isoformat()

    # Determinar turno del día basado en la hora del hecho
    hora = 0
    try:
        fecha_str = row.get("fecha_hecho", "")
        if "T" in fecha_str:
            hora = int(fecha_str.split("T")[1].split(":")[0])
        elif " " in fecha_str:
            hora = int(fecha_str.split(" ")[1].split(":")[0])
    except (ValueError, IndexError):
        hora = random.randint(0, 23)

    if 6 <= hora < 12:
        turno = "Mañana"
    elif 12 <= hora < 18:
        turno = "Tarde"
    elif 18 <= hora < 22:
        turno = "Noche"
    else:
        turno = "Madrugada"

    return {
        **row,
        "timestamp_ingesta": ahora,
        "turno_dia":         turno,
        "hora_hecho":        hora,
        "event_id":          f"EVT-{random.randint(100000, 999999)}",
        "fuente":            "SISC-Medellín",
    }


def publicar_eventos(producer, archivo: str) -> None:
    """Lee el CSV y publica cada fila como evento Kafka."""
    if not os.path.exists(archivo):
        raise FileNotFoundError(
            f"No se encontró {archivo}. Ejecuta primero: python 1_ingesta.py"
        )

    enviados = 0
    errores  = 0

    print(f"[PRODUCER] Publicando eventos desde: {archivo}")
    print(f"[PRODUCER] Tópico: {TOPIC}  |  Broker: {KAFKA_BROKER}")
    print("-" * 60)

    with open(archivo, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                mensaje = enriquecer_mensaje(dict(row))
                future  = producer.send(TOPIC, value=mensaje)
                future.get(timeout=10)   # Esperar confirmación
                enviados += 1

                if enviados % BATCH_SIZE == 0:
                    print(f"  [+] Enviados: {enviados:,} | Errores: {errores} | {datetime.now().strftime('%H:%M:%S')}")

                # Delay aleatorio para simular streaming real
                time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

            except Exception as e:
                errores += 1
                print(f"  [!] Error en registro {enviados}: {e}")

    producer.flush()
    print("-" * 60)
    print(f"[PRODUCER] ✓ Finalizado | Enviados: {enviados:,} | Errores: {errores}")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("  PIPELINE HURTOS MEDELLÍN — PASO 2: PRODUCER KAFKA")
    print("=" * 60)
    print("  Asegúrate de que Kafka esté corriendo:")
    print("  $ docker-compose up -d kafka zookeeper")
    print("=" * 60)

    producer = crear_producer()
    try:
        publicar_eventos(producer, INPUT_FILE)
    finally:
        producer.close()
        print("[PRODUCER] Conexión cerrada.")
