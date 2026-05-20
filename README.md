# 🔍 Pipeline de Análisis de Hurtos en Medellín

---

## 📋 Descripción del Proyecto

Pipeline de datos distribuido que analiza los hurtos registrados en Medellín
usando datos abiertos de la Alcaldía de Medellín (portal SISC / MEData).

**Problema de negocio:** ¿En qué comunas, horarios y modalidades se
concentran los hurtos en Medellín? ¿Cómo ha evolucionado la criminalidad?

---

## 🏗️ Arquitectura

```
┌─────────────────────────────────────────────────────────────┐
│                PIPELINE HURTOS MEDELLÍN                      │
│                                                             │
│  [datos.gov.co]                                             │
│       │                                                     │
│       ▼                                                     │
│  1_ingesta.py          → data/hurtos_medellin_raw.csv       │
│       │                                                     │
│       ▼                                                     │
│  2_producer_kafka.py   → Kafka (tópico: hurtos-medellin)    │
│       │                                                     │
│       ▼                                                     │
│  3_spark_processor.py  → Spark Structured Streaming         │
│       │                    (limpieza, agregaciones,         │
│       │                     detección zonas de riesgo)      │
│       ▼                                                     │
│  MongoDB               → colecciones: eventos_raw, agregados│
│       │                                                     │
│       ▼                                                     │
│  4_dashboard.py        → Dashboard Streamlit (web)          │
└─────────────────────────────────────────────────────────────┘
```

### Stack Tecnológico

| Componente   | Tecnología          | Versión  |
|--------------|---------------------|----------|
| Lenguaje     | Python              | 3.11+    |
| Streaming    | Apache Kafka        | 7.5.0    |
| Procesamiento| Apache Spark        | 3.5.0    |
| Almacenamiento| MongoDB            | 7.0      |
| Visualización| Streamlit           | 1.32+    |
| Contenedores | Docker Compose      | 3.8      |

---

## 🚀 Instalación y Ejecución

### Requisitos previos

- Python 3.11+
- Docker Desktop instalado y corriendo

### Paso 0: Instalar dependencias Python

```bash
pip install -r requirements.txt
```

### Paso 1: Levantar la infraestructura con Docker

```bash
docker-compose up -d
```

Espera ~30 segundos hasta que todos los servicios estén `healthy`.

```bash
docker-compose ps   # Verifica que todos estén Up
```

Servicios disponibles:
- **Kafka**: `localhost:9092`
- **MongoDB**: `localhost:27017`
- **Kafka UI**: `http://localhost:8080` (interfaz web de monitoreo)

### Paso 2: Descargar datos

```bash
python 1_ingesta.py
```

Descarga el dataset de hurtos desde `datos.gov.co`.  
Si la API no está disponible, genera automáticamente datos simulados realistas.

**Salida:** `data/hurtos_medellin_raw.csv`

### Paso 3: Publicar eventos en Kafka

```bash
python 2_producer_kafka.py
```

Lee el CSV y publica cada fila como evento JSON en el tópico `hurtos-medellin`.  
Monitorea en: http://localhost:8080

### Paso 4: Procesar con Spark

**Modo batch** (más rápido, recomendado para desarrollo):
```bash
python 3_spark_processor.py --mode batch
```

**Modo streaming** (requiere PySpark instalado):
```bash
python 3_spark_processor.py --mode streaming
```

**Salida:** MongoDB colecciones `eventos_raw` y `agregados`

### Paso 5: Ver el Dashboard

```bash
streamlit run 4_dashboard.py
```

Abre automáticamente: http://localhost:8501

---

## 📁 Estructura del Proyecto

```
pipeline_hurtos_medellin/
│
├── 1_ingesta.py              # Descarga de datos (API + fallback simulado)
├── 2_producer_kafka.py       # Publicación de eventos en Kafka
├── 3_spark_processor.py      # Procesamiento Spark (batch + streaming)
├── 4_dashboard.py            # Dashboard Streamlit interactivo
│
├── docker-compose.yml        # Infraestructura (Kafka + MongoDB + UI)
├── requirements.txt          # Dependencias Python
├── README.md                 # Este archivo
│
├── data/                     # Generado automáticamente
│   ├── hurtos_medellin_raw.csv
│   ├── resultados_procesados.json
│   └── ingesta_log.json
│
└── scripts/
    └── mongo-init.js         # Inicialización de MongoDB
```

---

## 📊 Dataset

- **Fuente:** Alcaldía de Medellín — Sistema de Información para la Seguridad y la Convivencia (SISC)
- **Portal:** https://www.datos.gov.co y https://medata.gov.co
- **Licencia:** Datos Abiertos Colombia (CC BY)
- **Contenido:** Hurtos a personas, motos, carros y residencias registrados por la Policía Nacional en Medellín

---

## 🔬 Preguntas Analíticas que Responde el Pipeline

1. ¿Cuáles son las comunas con mayor índice de hurtos?
2. ¿En qué turno del día ocurren más robos?
3. ¿Cuál es la tendencia anual de criminalidad?
4. ¿Qué modalidad de hurto es más común?
5. ¿Cómo se distribuyen geográficamente los hurtos?
6. ¿Qué comunas tienen nivel de riesgo ALTO, MEDIO o BAJO?

---

## 📚 Referencias

- Dataset Hurtos Medellín: https://www.datos.gov.co/dataset/Hurto-a-persona/nvfz-tncu/about_data
- MEData Alcaldía de Medellín: https://medata.gov.co
- Apache Kafka: https://kafka.apache.org
- Apache Spark: https://spark.apache.org
- MongoDB: https://www.mongodb.com
- Streamlit: https://streamlit.io
