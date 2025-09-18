from sklearn.cluster import KMeans

# --- Nombre de clusters (k) ---
k = 10

# --- Exécution de K-Means sur les features standardisées/log-transformées ---
kmeans = KMeans(n_clusters=k, random_state=42, n_init=50)
clusters = kmeans.fit_predict(X_scaled_df)

# Ajouter les clusters au DataFrame original
df['cluster_id'] = clusters

# --- Calcul de la moyenne RFM pour chaque cluster ---
cluster_rfm_summary = df.groupby('cluster_id')[['days_since_last_transaction', 'txn_count_90d', 'total_txn_volume_90d']].mean()
cluster_rfm_summary = cluster_rfm_summary.rename(columns={
    'days_since_last_transaction': 'recency_mean',
    'txn_count_90d': 'frequency_mean',
    'total_txn_volume_90d': 'monetary_mean'
}).sort_values(by=['recency_mean', 'frequency_mean', 'monetary_mean'], ascending=[True, False, False])

# --- Attribution des noms aux clusters selon leur profil moyen ---
cluster_names = [
    '01-Champions', '02-Loyaux Premium', '03-Grands Dépensiers', '04-Fidèles Modérés',
    '05-Clients Potentiels', '06-Moyens Actifs', '07-En Risque', '08-À Réactiver',
    '09-Dormants', '10-Perdus'
]

# Mapper cluster_id -> cluster_name
cluster_name_mapping = dict(zip(cluster_rfm_summary.index, cluster_names))
df['cluster_name'] = df['cluster_id'].map(cluster_name_mapping)

# --- Vérification ---
df[['clientid', 'cluster_id', 'cluster_name', 'days_since_last_transaction', 'txn_count_90d', 'total_txn_volume_90d']]
rfm_segmented = df[['clientid', 'cluster_id', 'cluster_name', 'days_since_last_transaction', 'txn_count_90d', 'total_txn_volume_90d']]
rfm_segmented.head(10)
