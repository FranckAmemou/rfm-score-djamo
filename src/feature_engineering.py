# feature_engineering.py
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
import os
import sys

# Ajouter le dossier src au path pour importer data_extraction
sys.path.append(os.path.join(os.path.dirname(__file__)))

def engineer_features(df_rfm_90d):
    """Effectue le feature engineering complet sur les données RFM"""
    print("🎯 Début du feature engineering...")
    
    # Copier le DataFrame RFM 90 jours
    df_engineered = df_rfm_90d.copy()
    
    # --- Log-transform sur R, F, M pour limiter l'effet des outliers ---
    df_engineered['log_recency'] = np.log1p(df_engineered['days_since_last_transaction'])
    df_engineered['log_frequency'] = np.log1p(df_engineered['txn_count_90d'])
    df_engineered['log_monetary'] = np.log1p(df_engineered['total_txn_volume_90d'])
    
    # --- Features supplémentaires ---
    # Ratio d'activité (jours actifs / jours total potentiels)
    df_engineered['activity_ratio'] = df_engineered['active_days_count_90d'] / 90
    
    # Volume transactionnel moyen par jour actif
    df_engineered['avg_daily_volume'] = np.where(
        df_engineered['active_days_count_90d'] > 0,
        df_engineered['total_txn_volume_90d'] / df_engineered['active_days_count_90d'],
        0
    )
    
    # Diversification des produits (poids relatif)
    df_engineered['product_diversification'] = df_engineered['nb_active_products'] / 3
    
    # Ratio de stabilité transactionnelle
    df_engineered['value_stability'] = 1 / (1 + df_engineered['value_consistency_std'])
    
    # Liquidité totale (solde courant + vault)
    df_engineered['total_liquidity'] = df_engineered['current_account_balance'] + df_engineered['vault_balance']
    
    # Ratio d'engagement (volume transactionnel / liquidité)
    df_engineered['engagement_ratio'] = np.where(
        df_engineered['total_liquidity'] > 0,
        df_engineered['total_txn_volume_90d'] / df_engineered['total_liquidity'],
        0
    )
    
    # --- Sélection des features pour clustering ---
    features_for_clustering = [
        'log_recency', 'log_frequency', 'log_monetary',
        'active_days_count_90d', 'nb_active_products', 'value_consistency_std',
        'txn_frequency_ratio', 'avg_total_txn_volume_90d', 'vault_balance',
        'aum_snapshot', 'total_assets_snapshot', 'balance_velocity_90d',
        'activity_ratio', 'avg_daily_volume', 'product_diversification',
        'value_stability', 'total_liquidity', 'engagement_ratio'
    ]
    
    # Nettoyage des valeurs infinies et NaN
    for col in features_for_clustering:
        if col in df_engineered.columns:
            df_engineered[col] = df_engineered[col].replace([np.inf, -np.inf], np.nan)
            df_engineered[col] = df_engineered[col].fillna(df_engineered[col].median())
    
    X = df_engineered[features_for_clustering].copy()
    
    # --- Standardisation ---
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    
    # Convertir en DataFrame
    X_scaled_df = pd.DataFrame(X_scaled, columns=features_for_clustering)
    X_scaled_df['clientid'] = df_engineered['clientid'].values
    
    print(f"✅ Feature engineering terminé: {len(features_for_clustering)} features créées")
    return df_engineered, X_scaled_df, scaler

if __name__ == "__main__":
    # Mode standalone: importe et transforme
    from data_extraction import extract_rfm_data
    
    df_rfm = extract_rfm_data()
    df_engineered, X_scaled, scaler = engineer_features(df_rfm)
    
    print(f"\n Données transformées: {df_engineered.shape}")
    print(f" Features scaled: {X_scaled.shape}")