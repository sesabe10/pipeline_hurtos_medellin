"""
=============================================================
PIPELINE DE HURTOS EN MEDELLÍN
Paso 4: Dashboard Interactivo — Datos Reales SISC Medellín
=============================================================
Uso: streamlit run 4_dashboard.py
"""

import json, os
from datetime import datetime
import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="Hurtos Medellín — Dashboard",
    page_icon="🔍", layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
.header-bar{background:linear-gradient(90deg,#1E2761,#4FC3F7);
            padding:18px 24px;border-radius:12px;color:white;margin-bottom:20px}
.header-bar h1{margin:0;font-size:1.6rem}
.header-bar p{margin:4px 0 0;opacity:.85;font-size:.85rem}
.kpi-label{font-size:.75rem;color:#64748b;margin:0}
.kpi-value{font-size:1.8rem;font-weight:700;margin:0;color:#1E2761}
</style>
""", unsafe_allow_html=True)

COMUNAS_MAP = {
    1:"Popular",2:"Santa Cruz",3:"Manrique",4:"Aranjuez",5:"Castilla",
    6:"Doce de Octubre",7:"Robledo",8:"Villa Hermosa",9:"Buenos Aires",
    10:"La Candelaria",11:"Laureles-Estadio",12:"La América",13:"San Javier",
    14:"El Poblado",15:"Guayabal",16:"Belén",50:"Palmitas",60:"San Cristóbal",
    70:"Altavista",80:"San Antonio de Prado",90:"Santa Elena"
}

# ─────────────────────────────────────────────
# CARGA DE DATOS
# ─────────────────────────────────────────────
@st.cache_data(ttl=30)
def cargar_datos():
    # 1. MongoDB
    try:
        from pymongo import MongoClient
        client = MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=2000)
        client.server_info()
        db  = client["hurtos_medellin"]
        raw = list(db["eventos_raw"].find({}, {"_id":0}))
        agg = db["agregados"].find_one({}, {"_id":0}) or {}
        df  = pd.DataFrame(raw)
        client.close()
        if not df.empty:
            return df, agg, "🟢 MongoDB (Live)"
    except Exception:
        pass

    # 2. CSV procesado
    if os.path.exists("data/hurtos_medellin_raw.csv"):
        df  = pd.read_csv("data/hurtos_medellin_raw.csv", low_memory=False)
        agg = {}
        if os.path.exists("data/resultados_procesados.json"):
            with open("data/resultados_procesados.json", encoding="utf-8") as f:
                agg = json.load(f)
        return df, agg, "🟡 CSV local"

    return pd.DataFrame(), {}, "❌ Sin datos"


# ─────────────────────────────────────────────
# SIDEBAR — FILTROS
# ─────────────────────────────────────────────
def aplicar_filtros(df: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.title("🔍 Filtros")

    # Comuna
    comunas = sorted(df["comuna"].dropna().unique())
    sel_c   = st.sidebar.multiselect("Comuna", comunas)
    if sel_c: df = df[df["comuna"].isin(sel_c)]

    # Turno
    turnos  = ["Mañana","Tarde","Noche","Madrugada"]
    sel_t   = st.sidebar.multiselect("Turno del día", turnos)
    if sel_t: df = df[df["turno_dia"].isin(sel_t)]

    # Modalidad
    if "modalidad" in df.columns:
        mods  = sorted(df["modalidad"].dropna().unique())
        sel_m = st.sidebar.multiselect("Modalidad", mods)
        if sel_m: df = df[df["modalidad"].isin(sel_m)]

    # Sexo víctima
    if "sexo" in df.columns:
        sexos  = sorted(df["sexo"].dropna().unique())
        sel_s  = st.sidebar.multiselect("Sexo víctima", sexos)
        if sel_s: df = df[df["sexo"].isin(sel_s)]

    # Rango de hora
    hora_min, hora_max = st.sidebar.slider("Hora del hecho", 0, 23, (0, 23))
    if "hora" in df.columns:
        df["hora"] = pd.to_numeric(df["hora"], errors="coerce")
        df = df[df["hora"].between(hora_min, hora_max)]

    st.sidebar.markdown("---")
    st.sidebar.caption("Fuente: SISC — Alcaldía de Medellín")
    st.sidebar.caption(f"Datos reales: 10.211 registros")
    st.sidebar.caption(f"Actualizado: {datetime.now().strftime('%H:%M:%S')}")
    return df


# ─────────────────────────────────────────────
# SECCIONES
# ─────────────────────────────────────────────
def seccion_kpis(df, fuente):
    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("📊 Total hurtos",        f"{len(df):,}")
    c2.metric("📍 Comuna más afectada", df["comuna"].value_counts().idxmax() if "comuna" in df.columns else "—")
    c3.metric("🕐 Turno más peligroso", df["turno_dia"].value_counts().idxmax() if "turno_dia" in df.columns else "—")
    c4.metric("💰 Bien más hurtado",    df["bien_hurtado"].value_counts().idxmax() if "bien_hurtado" in df.columns else "—")
    c5.metric("🔗 Fuente",             fuente)


def seccion_comunas_modalidad(df):
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("📍 Hurtos por Comuna")
        data = df["comuna"].value_counts().head(12).reset_index()
        data.columns = ["Comuna","Hurtos"]
        st.bar_chart(data.set_index("Comuna"))
    with c2:
        st.subheader("🎭 Modalidad de Hurto")
        data = df["modalidad"].value_counts().reset_index()
        data.columns = ["Modalidad","Total"]
        st.bar_chart(data.set_index("Modalidad"))


def seccion_tiempo(df):
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("🕐 Hurtos por Turno del Día")
        data = df["turno_dia"].value_counts().reset_index()
        data.columns = ["Turno","Total"]
        st.bar_chart(data.set_index("Turno"))
    with c2:
        st.subheader("⏰ Distribución por Hora")
        if "hora" in df.columns:
            data = df["hora"].value_counts().sort_index().reset_index()
            data.columns = ["Hora","Total"]
            st.line_chart(data.set_index("Hora"))


def seccion_victima_arma(df):
    c1, c2, c3 = st.columns(3)
    with c1:
        st.subheader("👤 Sexo de la Víctima")
        if "sexo" in df.columns:
            data = df["sexo"].value_counts().reset_index()
            data.columns = ["Sexo","Total"]
            st.bar_chart(data.set_index("Sexo"))
    with c2:
        st.subheader("🔫 Arma o Medio Usado")
        if "arma" in df.columns:
            data = df["arma"].value_counts().reset_index()
            data.columns = ["Arma","Total"]
            st.bar_chart(data.set_index("Arma"))
    with c3:
        st.subheader("🚌 Transporte de la Víctima")
        if "transporte_victima" in df.columns:
            data = df["transporte_victima"].value_counts().head(8).reset_index()
            data.columns = ["Transporte","Total"]
            st.bar_chart(data.set_index("Transporte"))


def seccion_bien_lugar(df):
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("💰 Bienes Hurtados")
        if "bien_hurtado" in df.columns:
            data = df["bien_hurtado"].value_counts().head(10).reset_index()
            data.columns = ["Bien","Total"]
            st.bar_chart(data.set_index("Bien"))
    with c2:
        st.subheader("📍 Lugar del Hecho")
        if "lugar" in df.columns:
            data = df["lugar"].value_counts().head(8).reset_index()
            data.columns = ["Lugar","Total"]
            st.bar_chart(data.set_index("Lugar"))


def seccion_riesgo_barrio(df):
    c1, c2 = st.columns([1,2])
    with c1:
        st.subheader("🚨 Nivel de Riesgo por Comuna")
        if "zona_riesgo" in df.columns and "comuna" in df.columns:
            tabla = (df.groupby(["comuna","zona_riesgo"])
                       .size()
                       .reset_index(name="Hurtos")
                       .sort_values("Hurtos", ascending=False))
            tabla["Zona"] = tabla["zona_riesgo"].map(
                {"ALTO":"🔴 ALTO","MEDIO":"🟠 MEDIO","BAJO":"🟢 BAJO"}
            )
            st.dataframe(
                tabla[["comuna","Zona","Hurtos"]].rename(columns={"comuna":"Comuna"}),
                use_container_width=True, hide_index=True
            )
    with c2:
        st.subheader("🗺️ Mapa de Hurtos en Medellín")
        if "lat" in df.columns and "lon" in df.columns:
            mapa = df[["lat","lon"]].dropna()
            mapa["lat"] = pd.to_numeric(mapa["lat"], errors="coerce")
            mapa["lon"] = pd.to_numeric(mapa["lon"], errors="coerce")
            mapa = mapa.dropna()
            mapa = mapa[mapa["lat"].between(6.10,6.45) & mapa["lon"].between(-75.75,-75.45)]
            if not mapa.empty:
                st.map(mapa, zoom=12)
            else:
                st.info("No hay coordenadas válidas con los filtros actuales.")


def seccion_top_barrios(df):
    st.subheader("🏘️ Top 15 Barrios con Más Hurtos")
    if "barrio" in df.columns:
        data = (df["barrio"].value_counts()
                  .head(15)
                  .reset_index())
        data.columns = ["Barrio","Hurtos"]
        st.dataframe(data, use_container_width=True, hide_index=True)


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    st.markdown("""
    <div class="header-bar">
        <h1>🔍 Pipeline de Análisis de Hurtos — Medellín</h1>
        <p>Datos reales SISC · Institución Universitaria Pascual Bravo ·
           Gestión de Datos Masivos · Stack: Kafka → Spark → MongoDB → Streamlit</p>
    </div>
    """, unsafe_allow_html=True)

    with st.spinner("Cargando datos reales del pipeline..."):
        df, agg, fuente = cargar_datos()

    if df.empty:
        st.error("No hay datos. Ejecuta primero: python 1_ingesta.py && python 3_spark_processor.py")
        return

    # Asegurar tipos numéricos
    for col_num in ["hora","edad","codigo_comuna"]:
        if col_num in df.columns:
            df[col_num] = pd.to_numeric(df[col_num], errors="coerce")

    df = aplicar_filtros(df)

    seccion_kpis(df, fuente)
    st.markdown("---")
    seccion_comunas_modalidad(df)
    st.markdown("---")
    seccion_tiempo(df)
    st.markdown("---")
    seccion_victima_arma(df)
    st.markdown("---")
    seccion_bien_lugar(df)
    st.markdown("---")
    seccion_riesgo_barrio(df)
    st.markdown("---")
    seccion_top_barrios(df)

    st.markdown("---")
    st.caption(
        f"Fuente: SISC — Secretaría de Seguridad de Medellín · "
        f"Registros: {len(df):,} · "
        f"Conexión: {fuente} · "
        f"Actualizado: {datetime.now().strftime('%d/%m/%Y %H:%M')}"
    )
    if st.button("🔄 Actualizar"):
        st.cache_data.clear()
        st.rerun()


if __name__ == "__main__":
    main()
