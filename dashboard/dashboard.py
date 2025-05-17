import streamlit as st
import pandas as pd
import os

# --- Titre
st.title("📊 Dashboard météo - Moyennes")

# --- Choix de la période
option = st.radio("Afficher les moyennes par :", ["Heure", "Jour"])

# --- Chemin Parquet selon le choix
parquet_path = "./../consumers/resultats/weather_hourly" if option == "Heure" else "./../consumers/resultats/weather_daily"

if os.path.exists(parquet_path):
    df = pd.read_parquet(parquet_path)

    # --- Vérifier les colonnes disponibles
    st.write("Colonnes disponibles :", df.columns)
    st.write("Données chargées :")
    st.write(df.head()) 

    # --- Si 'window' est une struct, extrait 'start' de la fenêtre
    if 'window' in df.columns:
        df['window_start'] = df['window'].apply(lambda x: x['start'] if isinstance(x, dict) else None)
    st.write(df['window'].head())  
    # --- Afficher les graphiques
    st.subheader("Température moyenne")
    st.line_chart(df.set_index("window_start")["avg_temp"])

    st.subheader("Humidité moyenne")
    st.line_chart(df.set_index("window_start")["avg_humidity"])

    st.subheader("Vitesse du vent moyenne")
    st.line_chart(df.set_index("window_start")["avg_wind_speed"])

else:
    st.warning("Les données ne sont pas encore disponibles.")