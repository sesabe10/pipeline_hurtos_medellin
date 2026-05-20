"""
=============================================================
PIPELINE DE HURTOS EN MEDELLÍN
Paso 1: Ingesta y Limpieza de Datos Reales
Fuente: Dataset oficial SISC Medellín (robos_dataset.csv)
=============================================================
Lee el CSV real, lo limpia y lo deja listo para el pipeline.
10.211 registros reales de hurtos a personas en Medellín.
"""

import pandas as pd
import os
import json
from datetime import datetime

# ─────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────
INPUT_FILE  = "data/robos_dataset.csv"
OUTPUT_FILE = "data/hurtos_medellin_raw.csv"

COMUNAS_MAP = {
    1: "Popular",           2: "Santa Cruz",        3: "Manrique",
    4: "Aranjuez",          5: "Castilla",           6: "Doce de Octubre",
    7: "Robledo",           8: "Villa Hermosa",      9: "Buenos Aires",
    10: "La Candelaria",    11: "Laureles-Estadio",  12: "La América",
    13: "San Javier",       14: "El Poblado",        15: "Guayabal",
    16: "Belén",            50: "Palmitas",           60: "San Cristóbal",
    70: "Altavista",        80: "San Antonio de Prado", 90: "Santa Elena"
}

# ─────────────────────────────────────────────
# FUNCIONES
# ─────────────────────────────────────────────

def cargar_datos(path: str) -> pd.DataFrame:
    print(f"[INGESTA] Leyendo dataset real: {path}")
    df = pd.read_csv(path, sep=";", low_memory=False, encoding="utf-8-sig")
    print(f"[INGESTA] Registros cargados: {len(df):,}")
    print(f"[INGESTA] Columnas: {list(df.columns)}")
    return df


def limpiar_datos(df: pd.DataFrame) -> pd.DataFrame:
    print("[INGESTA] Iniciando limpieza...")

    # 1. Parsear fecha
    df["fecha_dt"] = pd.to_datetime(
        df["fecha_hecho"], format="%d/%m/%Y %H:%M", errors="coerce"
    )
    nulos_fecha = df["fecha_dt"].isna().sum()
    if nulos_fecha:
        print(f"  → Fechas no parseadas (eliminadas): {nulos_fecha}")
    df = df.dropna(subset=["fecha_dt"])

    # 2. Extraer componentes temporales
    df["anio"]     = df["fecha_dt"].dt.year
    df["mes"]      = df["fecha_dt"].dt.month
    df["hora"]     = df["fecha_dt"].dt.hour
    df["dia_semana"] = df["fecha_dt"].dt.day_name()

    # 3. Turno del día
    def turno(h):
        if 6 <= h < 12:  return "Mañana"
        if 12 <= h < 18: return "Tarde"
        if 18 <= h < 22: return "Noche"
        return "Madrugada"
    df["turno_dia"] = df["hora"].apply(turno)

    # 4. Nombre de comuna desde código
    df["nombre_comuna"] = df["codigo_comuna"].map(COMUNAS_MAP).fillna("Desconocida")

    # 5. Corregir coordenadas (están sin punto decimal en el CSV)
    # Latitud real Medellín ≈ 6.25  → raw ≈ 625000000  → dividir /1e8
    # Longitud real Medellín ≈ -75.6 → raw ≈ -7560000000 → dividir /1e8
    def corregir_coord(serie, divisor=1e8):
        return pd.to_numeric(serie, errors="coerce") / divisor

    df["lat"] = corregir_coord(df["latitud"])
    df["lon"] = corregir_coord(df["longitud"])

    # Filtrar coordenadas fuera de Medellín (bbox real)
    mask_coords = (
        df["lat"].between(6.10, 6.45) &
        df["lon"].between(-75.75, -75.45)
    )
    coords_invalidas = (~mask_coords & df["lat"].notna()).sum()
    df.loc[~mask_coords, ["lat", "lon"]] = None
    print(f"  → Coordenadas fuera de Medellín (anuladas): {coords_invalidas}")

    # 6. Estandarizar valores "Sin dato"
    for col in ["modalidad", "arma_medio", "conducta_especial",
                "medio_transporte", "sexo", "estado_civil"]:
        if col in df.columns:
            df[col] = df[col].replace("Sin dato", "No especificado")

    # 7. Zona de riesgo por frecuencia de comuna
    conteo = df["nombre_comuna"].value_counts()
    p75 = conteo.quantile(0.75)
    p50 = conteo.quantile(0.50)
    df["zona_riesgo"] = df["nombre_comuna"].map(
        lambda c: "ALTO" if conteo.get(c, 0) >= p75
                  else ("MEDIO" if conteo.get(c, 0) >= p50 else "BAJO")
    )

    # 8. Eliminar duplicados
    antes = len(df)
    df = df.drop_duplicates()
    print(f"  → Duplicados eliminados: {antes - len(df)}")

    # 9. Seleccionar y renombrar columnas finales
    df = df.rename(columns={
        "nombre_barrio":    "barrio",
        "nombre_comuna":    "comuna",
        "bien":             "bien_hurtado",
        "categoria_bien":   "categoria_bien",
        "arma_medio":       "arma",
        "conducta_especial":"tipo_conducta",
        "medio_transporte": "transporte_victima",
    })

    columnas_finales = [
        "fecha_hecho", "fecha_dt", "anio", "mes", "hora", "dia_semana", "turno_dia",
        "comuna", "barrio", "codigo_comuna",
        "modalidad", "tipo_conducta", "arma", "transporte_victima",
        "bien_hurtado", "categoria_bien", "grupo_bien",
        "lugar", "sexo", "edad", "estado_civil",
        "lat", "lon", "zona_riesgo"
    ]
    columnas_existentes = [c for c in columnas_finales if c in df.columns]
    df = df[columnas_existentes]

    print(f"[INGESTA] Registros limpios listos: {len(df):,}")
    return df


def guardar_datos(df: pd.DataFrame, path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8")
    print(f"[INGESTA] ✓ Guardado → {path}")


def imprimir_resumen(df: pd.DataFrame):
    print("\n" + "="*50)
    print("  RESUMEN DEL DATASET")
    print("="*50)
    print(f"  Total registros:     {len(df):,}")
    print(f"  Rango fechas:        {df['fecha_dt'].min()} → {df['fecha_dt'].max()}")
    print(f"  Comunas cubiertas:   {df['comuna'].nunique()}")
    print(f"  Barrios cubiertos:   {df['barrio'].nunique()}")
    print(f"\n  Top 5 comunas más afectadas:")
    for c, n in df["comuna"].value_counts().head(5).items():
        print(f"    {c:<25} {n:>5,} hurtos")
    print(f"\n  Modalidades:")
    for m, n in df["modalidad"].value_counts().head(5).items():
        print(f"    {m:<25} {n:>5,}")
    print(f"\n  Turno más peligroso: {df['turno_dia'].value_counts().idxmax()}")
    print(f"  Bien más hurtado:    {df['bien_hurtado'].value_counts().idxmax()}")
    print("="*50)


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
if __name__ == "__main__":
    print("="*60)
    print("  PIPELINE HURTOS MEDELLÍN — PASO 1: INGESTA DATOS REALES")
    print("="*60)

    if not os.path.exists(INPUT_FILE):
        raise FileNotFoundError(
            f"No se encontró {INPUT_FILE}\n"
            "Copia robos_dataset.csv a la carpeta data/"
        )

    df_raw   = cargar_datos(INPUT_FILE)
    df_clean = limpiar_datos(df_raw)
    guardar_datos(df_clean, OUTPUT_FILE)
    imprimir_resumen(df_clean)

    resumen = {
        "timestamp":        datetime.now().isoformat(),
        "fuente":           INPUT_FILE,
        "total_registros":  len(df_clean),
        "columnas":         list(df_clean.columns),
        "comunas":          df_clean["comuna"].value_counts().to_dict(),
        "modalidades":      df_clean["modalidad"].value_counts().to_dict(),
        "archivo_salida":   OUTPUT_FILE,
    }
    with open("data/ingesta_log.json", "w", encoding="utf-8") as f:
        json.dump(resumen, f, indent=2, ensure_ascii=False, default=str)

    print("\n[INGESTA] ✓ Completado. Ejecuta: python 2_producer_kafka.py")
