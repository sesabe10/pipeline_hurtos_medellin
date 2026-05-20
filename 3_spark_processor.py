"""
=============================================================
PIPELINE DE HURTOS EN MEDELLÍN
Paso 3: Procesamiento con Apache Spark
Datos reales: 10.106 registros SISC Medellín
=============================================================
Uso:
  python 3_spark_processor.py --mode batch
  python 3_spark_processor.py --mode streaming
"""

import argparse, json, os
from datetime import datetime

KAFKA_BROKER   = "localhost:9092"
KAFKA_TOPIC    = "hurtos-medellin"
MONGO_URI      = "mongodb://localhost:27017/"
MONGO_DB       = "hurtos_medellin"
MONGO_COLL_RAW = "eventos_raw"
MONGO_COLL_AGG = "agregados"
INPUT_CSV      = "data/hurtos_medellin_raw.csv"

# ─────────────────────────────────────────────
# MODO BATCH (pandas — fallback local)
# ─────────────────────────────────────────────
def procesar_batch_pandas(input_csv: str):
    import pandas as pd
    from pymongo import MongoClient

    print("[SPARK-BATCH] Procesando datos reales con pandas...")
    df = pd.read_csv(input_csv, low_memory=False)
    print(f"[SPARK-BATCH] Registros cargados: {len(df):,}")

    # ── Agregaciones ─────────────────────────────────────────────────

    # 1. Por comuna
    por_comuna = (df.groupby("comuna")
                    .agg(total_hurtos=("comuna","count"))
                    .reset_index()
                    .sort_values("total_hurtos", ascending=False))

    # 2. Por turno del día
    por_turno = (df["turno_dia"].value_counts()
                   .reset_index()
                   .rename(columns={"turno_dia":"turno","count":"total"}))

    # 3. Por modalidad
    por_modalidad = (df["modalidad"].value_counts()
                       .reset_index()
                       .rename(columns={"modalidad":"modalidad","count":"total"}))

    # 4. Por hora del día
    por_hora = (df["hora"].value_counts()
                  .sort_index()
                  .reset_index()
                  .rename(columns={"hora":"hora","count":"total"}))

    # 5. Por bien hurtado
    por_bien = (df["bien_hurtado"].value_counts()
                  .head(10)
                  .reset_index()
                  .rename(columns={"bien_hurtado":"bien","count":"total"}))

    # 6. Por arma usada
    por_arma = (df["arma"].value_counts()
                  .reset_index()
                  .rename(columns={"arma":"arma","count":"total"}))

    # 7. Por lugar del hecho
    por_lugar = (df["lugar"].value_counts()
                   .head(8)
                   .reset_index()
                   .rename(columns={"lugar":"lugar","count":"total"}))

    # 8. Por sexo de víctima
    por_sexo = (df["sexo"].value_counts()
                  .reset_index()
                  .rename(columns={"sexo":"sexo","count":"total"}))

    # 9. Distribución de edad
    df["rango_edad"] = pd.cut(
        df["edad"],
        bins=[0,17,25,35,50,65,120],
        labels=["<18","18-25","26-35","36-50","51-65","65+"]
    )
    por_edad = (df["rango_edad"].value_counts()
                  .reset_index()
                  .rename(columns={"rango_edad":"rango","count":"total"}))

    # 10. Por barrio (top 15)
    por_barrio = (df.groupby("barrio")
                    .agg(total=("barrio","count"))
                    .reset_index()
                    .sort_values("total", ascending=False)
                    .head(15))

    # 11. Zona de riesgo resumen
    zona_riesgo = (df.groupby(["comuna","zona_riesgo"])
                     .agg(total=("comuna","count"))
                     .reset_index()
                     .sort_values("total", ascending=False))

    # ── Imprimir en consola ───────────────────────────────────────────
    print("\n" + "="*55)
    print("  ESTADÍSTICAS FINALES — DATOS REALES MEDELLÍN")
    print("="*55)
    print(f"  Total eventos procesados:  {len(df):,}")
    print(f"  Comuna más afectada:       {por_comuna.iloc[0]['comuna']}")
    print(f"  Turno más peligroso:       {por_turno.iloc[0]['turno']}")
    print(f"  Modalidad principal:       {por_modalidad.iloc[0]['modalidad']}")
    print(f"  Bien más hurtado:          {por_bien.iloc[0]['bien']}")
    print(f"\n  Top 5 comunas:")
    for _, r in por_comuna.head(5).iterrows():
        barra = "█" * int(r["total_hurtos"] / 100)
        print(f"    {r['comuna']:<25} {int(r['total_hurtos']):>5,}  {barra}")
    print("="*55)

    resultados = {
        "por_comuna":    por_comuna.to_dict("records"),
        "por_turno":     por_turno.to_dict("records"),
        "por_modalidad": por_modalidad.to_dict("records"),
        "por_hora":      por_hora.to_dict("records"),
        "por_bien":      por_bien.to_dict("records"),
        "por_arma":      por_arma.to_dict("records"),
        "por_lugar":     por_lugar.to_dict("records"),
        "por_sexo":      por_sexo.to_dict("records"),
        "por_edad":      por_edad.to_dict("records"),
        "por_barrio":    por_barrio.to_dict("records"),
        "zona_riesgo":   zona_riesgo.to_dict("records"),
    }

    # ── Guardar en MongoDB ────────────────────────────────────────────
    try:
        from pymongo import MongoClient
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
        client.server_info()
        db = client[MONGO_DB]

        db[MONGO_COLL_RAW].drop()
        registros = df.to_dict("records")
        db[MONGO_COLL_RAW].insert_many(registros, ordered=False)
        print(f"[MONGO] ✓ {len(registros):,} eventos → colección '{MONGO_COLL_RAW}'")

        doc_agg = {
            "procesado_en":  datetime.now().isoformat(),
            "total_eventos": len(df),
            "fuente":        "SISC Medellín — datos reales",
            **resultados,
        }
        db[MONGO_COLL_AGG].replace_one({}, doc_agg, upsert=True)
        print(f"[MONGO] ✓ Agregados → colección '{MONGO_COLL_AGG}'")
        client.close()

    except Exception as e:
        print(f"[MONGO] ⚠ No disponible ({e}). Guardando en JSON local.")
        os.makedirs("data", exist_ok=True)
        with open("data/resultados_procesados.json", "w", encoding="utf-8") as f:
            json.dump(
                {"procesado_en": datetime.now().isoformat(),
                 "total_eventos": len(df),
                 "fuente": "SISC Medellín — datos reales",
                 **resultados},
                f, indent=2, ensure_ascii=False, default=str
            )
        print("[LOCAL] ✓ data/resultados_procesados.json")

    return df, resultados


# ─────────────────────────────────────────────
# MODO STREAMING (PySpark real)
# ─────────────────────────────────────────────
def procesar_streaming_spark():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col, from_json, to_timestamp, hour, month, year,
        when, current_timestamp
    )
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType

    spark = (SparkSession.builder
             .appName("HurtosMedellinPipeline")
             .config("spark.jars.packages",
                     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                     "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0")
             .config("spark.mongodb.write.connection.uri", MONGO_URI)
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    print("[SPARK] ✓ SparkSession iniciada")

    schema = StructType([
        StructField("fecha_hecho",       StringType()),
        StructField("comuna",            StringType()),
        StructField("barrio",            StringType()),
        StructField("modalidad",         StringType()),
        StructField("bien_hurtado",      StringType()),
        StructField("arma",              StringType()),
        StructField("lugar",             StringType()),
        StructField("sexo",              StringType()),
        StructField("edad",              StringType()),
        StructField("turno_dia",         StringType()),
        StructField("hora",              StringType()),
        StructField("zona_riesgo",       StringType()),
        StructField("lat",               DoubleType()),
        StructField("lon",               DoubleType()),
        StructField("event_id",          StringType()),
        StructField("timestamp_ingesta", StringType()),
    ])

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BROKER)
           .option("subscribe", KAFKA_TOPIC)
           .option("startingOffsets", "earliest")
           .option("failOnDataLoss", "false")
           .load())

    eventos = (raw
               .selectExpr("CAST(value AS STRING) as json_str", "timestamp as kafka_ts")
               .select(from_json(col("json_str"), schema).alias("d"), "kafka_ts")
               .select("d.*", "kafka_ts"))

    procesado = (eventos
                 .withColumn("fecha_dt",     to_timestamp(col("fecha_hecho"), "d/MM/yyyy H:mm"))
                 .withColumn("anio",         year(col("fecha_dt")))
                 .withColumn("mes",          month(col("fecha_dt")))
                 .withColumn("hora_num",     hour(col("fecha_dt")))
                 .withColumn("procesado_en", current_timestamp()))

    def escribir_mongo(batch_df, batch_id):
        if batch_df.isEmpty():
            return
        (batch_df.write.format("mongodb").mode("append")
         .option("database", MONGO_DB)
         .option("collection", MONGO_COLL_RAW)
         .save())
        print(f"[SPARK] Lote {batch_id} → {batch_df.count():,} registros → MongoDB")

    query = (procesado.writeStream
             .foreachBatch(escribir_mongo)
             .option("checkpointLocation", "/tmp/spark_checkpoints/hurtos")
             .trigger(processingTime="10 seconds")
             .start())

    print("[SPARK] ✓ Streaming activo. Ctrl+C para detener.")
    query.awaitTermination()


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["batch","streaming"], default="batch")
    args = parser.parse_args()

    print("="*60)
    print("  PIPELINE HURTOS MEDELLÍN — PASO 3: SPARK PROCESSOR")
    print(f"  Modo: {args.mode.upper()} | Fuente: datos reales SISC")
    print("="*60)

    if args.mode == "streaming":
        procesar_streaming_spark()
    else:
        df, _ = procesar_batch_pandas(INPUT_CSV)
        print("\n[SPARK-BATCH] ✓ Completado. Ejecuta: streamlit run 4_dashboard.py")
