import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler

# Copier le DataFrame RFM 90 jours
df = df_rfm_90d.copy()

# --- Log-transform sur R, F, M pour limiter l'effet des outliers ---
df['log_recency'] = np.log1p(df['days_since_last_transaction'])
df['log_frequency'] = np.log1p(df['txn_count_90d'])
df['log_monetary'] = np.log1p(df['total_txn_volume_90d'])

# --- Sélection des features pour clustering ---
features_for_clustering = [
    'log_recency',
    'log_frequency',
    'log_monetary',
    'active_days_count_90d',
    'nb_active_products',
    'value_consistency_std',
    'txn_frequency_ratio',
    'avg_total_txn_volume_90d',
    'vault_balance',
    'aum_snapshot',
    'total_assets_snapshot',
    'balance_velocity_90d'
]

X = df[features_for_clustering].copy()

# --- Standardisation pour mettre toutes les features sur la même échelle ---
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# Convertir en DataFrame pour garder les noms de colonnes et l'index clientid
X_scaled_df = pd.DataFrame(X_scaled, columns=features_for_clustering, index=df['clientid'])

# Vérification
X_scaled_df.head()
