# rfm_pipeline.py
import pandas as pd
from data_extraction import extract_rfm_data
from feature_engineering import engineer_features
from clustering import run_clustering
from visualization1 import display_cluster_summary

# --- Étape 1 : Extraction ---
print("⏳ Extraction des données RFM 90 jours...")
df_rfm = extract_rfm_data()

# --- Étape 2 : Feature engineering ---
print("⏳ Feature engineering...")
df_engineered, X_scaled_df, scaler = engineer_features(df_rfm)

# --- Étape 3 : Clustering ---
print("⏳ Clustering K-Means...")
rfm_segmented = run_clustering(df_engineered, X_scaled_df)

# --- Étape 4 : Visualisation ---
print("⏳ Génération du résumé des clusters...")
cluster_summary = display_cluster_summary(rfm_segmented)

# --- Étape 5 : Affichage stylé ---
styled_table = cluster_summary.style.background_gradient(cmap='YlGnBu', subset=[
    'recency_mean', 'frequency_mean', 'monetary_mean'
]).format({
    'recency_mean': '{:.1f}',
    'recency_median': '{:.1f}',
    'frequency_mean': '{:.1f}',
    'frequency_median': '{:.1f}',
    'monetary_mean': '{:,.0f}',
    'monetary_median': '{:,.0f}',
    'pct_client': '{:.2f}%'
})
print(cluster_summary)
#print(styled_table)
