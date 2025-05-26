import streamlit as st
import pandas as pd
import os
import matplotlib.pyplot as plt

st.set_page_config(page_title="Dashboard météo", layout="wide")

# --- Titre principal
st.title("🌦️ Dashboard météo – Historique & Prédictions")

st.markdown("---")

# ========================
# 📊 PARTIE 1 : MOYENNES HISTORIQUES
# ========================

st.header("📈 Moyennes historiques")

# --- Choix de la période
option = st.radio("Afficher les moyennes par :", ["Heure", "Jour"], horizontal=True)

# --- Chemin des fichiers Parquet
parquet_path = "./../resultats/weather_hourly" if option == "Heure" else "./../resultats/weather_daily"

if os.path.exists(parquet_path):
    df = pd.read_parquet(parquet_path)

    # Vérification et extraction de la colonne temporelle
    if "window" in df.columns and isinstance(df["window"].iloc[0], dict):
        df["window_start"] = df["window"].apply(lambda x: x["start"])
    elif "hour_start" in df.columns:
        df["window_start"] = pd.to_datetime(df["hour_start"])
    elif "day_start" in df.columns:
        df["window_start"] = pd.to_datetime(df["day_start"])

    df = df.sort_values("window_start")

    st.subheader("🌡️ Température moyenne")
    st.line_chart(df.set_index("window_start")["avg_temp"])

    st.subheader("💧 Humidité moyenne")
    st.line_chart(df.set_index("window_start")["avg_humidity"])

    if "avg_wind_speed" in df.columns:
        st.subheader("💨 Vitesse moyenne du vent")
        st.line_chart(df.set_index("window_start")["avg_wind_speed"])

else:
    st.warning("Les données historiques ne sont pas encore disponibles.")

st.markdown("---")

# ========================
# 🔮 PARTIE 2 : PRÉDICTIONS
# ========================

st.header("🔮 Prévisions météo pour les 4 prochaines heures")

pred_path = "./../consumers/resultats/predictions/last_predictions.csv"

if not os.path.exists(pred_path):
    st.info("Aucune prédiction disponible pour l’instant.")
else:
    df_pred = pd.read_csv(pred_path)

    if "generated_at" in df_pred.columns:
        st.caption(f"📅 Généré le : `{df_pred['generated_at'][0]}`")

    variables = ["temperature", "humidity", "pressure", "wind_speed", "wind_deg"]
    heures = ["H+1", "H+2", "H+3", "H+4"]

    cols = st.columns(len(variables))
    for idx, var in enumerate(variables):
        values = df_pred[[f"{var}_h{i}" for i in range(1, 5)]].values[0]
        with cols[idx]:
            st.metric(label=var.capitalize(), value=f"{values[0]:.2f}")
            fig, ax = plt.subplots()
            ax.plot(heures, values, marker='o')
            ax.set_title(var)
            ax.set_ylabel(var)
            st.pyplot(fig)

    if st.button("🔁 Recharger les prédictions"):
        st.success("(Simulation) Les prédictions seraient recalculées ici.")
