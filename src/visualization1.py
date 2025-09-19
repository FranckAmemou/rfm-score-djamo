# visualization1.py
import pandas as pd

def display_cluster_summary(df):
    """
    Calcule et retourne le résumé des clusters
    """
    df_copy = df.copy()

    cluster_summary = df_copy.groupby('cluster_name').agg(
        recency_mean=('days_since_last_transaction', 'mean'),
        recency_median=('days_since_last_transaction', 'median'),
        frequency_mean=('txn_count_90d', 'mean'),
        frequency_median=('txn_count_90d', 'median'),
        monetary_mean=('total_txn_volume_90d', 'mean'),
        monetary_median=('total_txn_volume_90d', 'median'),
        nb_clients=('clientid', 'count')
    ).reset_index()

    total_clients = cluster_summary['nb_clients'].sum()
    cluster_summary['pct_client'] = (cluster_summary['nb_clients'] / total_clients * 100).round(2)

    cluster_order = [
        '01-Champions', '02-Loyaux Premium', '03-Grands Dépensiers', '04-Fidèles Modérés',
        '05-Clients Potentiels', '06-Moyens Actifs', '07-En Risque', '08-À Réactiver',
        '09-Dormants', '10-Perdus'
    ]
    cluster_summary['cluster_name'] = pd.Categorical(cluster_summary['cluster_name'], categories=cluster_order, ordered=True)
    cluster_summary = cluster_summary.sort_values('cluster_name')

    return cluster_summary
