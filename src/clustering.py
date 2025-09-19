# clustering.py
from sklearn.cluster import KMeans

def run_clustering(df_engineered, X_scaled_df, k=10):
    """
    Exécute K-Means et renvoie le DataFrame avec clusters attribués
    """
    print("🔎 Clustering en cours...")
    
    # --- Assure-toi que seules les features numériques sont utilisées ---
    X_features = X_scaled_df.drop(columns=['clientid'], errors='ignore')
    
    kmeans = KMeans(n_clusters=k, random_state=42, n_init=50)
    clusters = kmeans.fit_predict(X_features)
    
    # Ajouter les clusters au DataFrame original
    df = df_engineered.copy()
    df['cluster_id'] = clusters

    # --- Attribution des noms selon l'ordre moyen RFM ---
    cluster_rfm_summary = df.groupby('cluster_id')[['days_since_last_transaction', 'txn_count_90d', 'total_txn_volume_90d']].mean()
    cluster_rfm_summary = cluster_rfm_summary.rename(columns={
        'days_since_last_transaction': 'recency_mean',
        'txn_count_90d': 'frequency_mean',
        'total_txn_volume_90d': 'monetary_mean'
    }).sort_values(by=['recency_mean', 'frequency_mean', 'monetary_mean'], ascending=[True, False, False])

    cluster_names = [
        '01-Champions', '02-Loyaux Premium', '03-Grands Dépensiers', '04-Fidèles Modérés',
        '05-Clients Potentiels', '06-Moyens Actifs', '07-En Risque', '08-À Réactiver',
        '09-Dormants', '10-Perdus'
    ]

    cluster_name_mapping = dict(zip(cluster_rfm_summary.index, cluster_names))
    df['cluster_name'] = df['cluster_id'].map(cluster_name_mapping)
    
    print("✅ Clustering terminé!")
    return df
