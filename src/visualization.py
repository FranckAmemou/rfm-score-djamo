import pandas as pd

# --- Calcul des statistiques par cluster ---
cluster_summary = df.groupby('cluster_name').agg(
    recency_mean=('days_since_last_transaction', 'mean'),
    recency_median=('days_since_last_transaction', 'median'),
    frequency_mean=('txn_count_90d', 'mean'),
    frequency_median=('txn_count_90d', 'median'),
    monetary_mean=('total_txn_volume_90d', 'mean'),
    monetary_median=('total_txn_volume_90d', 'median'),
    nb_clients=('clientid', 'count')
).reset_index()

# --- Pourcentage de clients par cluster ---
total_clients = cluster_summary['nb_clients'].sum()
cluster_summary['pct_client'] = (cluster_summary['nb_clients'] / total_clients * 100).round(2)

# --- Tri pour un ordre cohérent (ex: Champions en premier) ---
cluster_order = [
    '01-Champions', '02-Loyaux Premium', '03-Grands Dépensiers', '04-Fidèles Modérés',
    '05-Clients Potentiels', '06-Moyens Actifs', '07-En Risque', '08-À Réactiver',
    '09-Dormants', '10-Perdus'
]
cluster_summary['cluster_name'] = pd.Categorical(cluster_summary['cluster_name'], categories=cluster_order, ordered=True)
cluster_summary = cluster_summary.sort_values('cluster_name')

# --- Tableau coloré (style pandas) ---
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

styled_table

#d'autre graph

import pandas as pd

# --- Profils détaillés pour les 10 segments RFM Djamo ---
segment_profiles = {
    '01-Champions': {
        'profil': 'Clients très récents et très dépensiers, multi-produits',
        'recency': 'Très faible (0-5 jours)',
        'valeur': 'Très élevée',
        'potentiel': 'Maintien et cross-selling'
    },
    '02-Loyaux Premium': {
        'profil': 'Fidèles, transactions régulières, actifs sur plusieurs produits',
        'recency': 'fible (5-15 jours)',
        'valeur': 'Élevée',
        'potentiel': 'Upselling et fidélisation'
    },
    '03-Grands Dépensiers': {
        'profil': 'Volume élevé mais activité irrégulière',
        'recency': 'Variable',
        'valeur': 'Élevée',
        'potentiel': 'Optimisation de la fréquence'
    },
    '04-Fidèles Modérés': {
        'profil': 'Transactions régulières, volume moyen',
        'recency': 'Faible à moyenne (10-30 jours)',
        'valeur': 'Moyenne',
        'potentiel': 'Rétention et cross-selling'
    },
    '05-Clients Potentiels': {
        'profil': 'clients avec potentiel d’augmentation du volume',
        'recency': 'Très faible (<7 jours)',
        'valeur': 'Faible mais croissante',
        'potentiel': 'Conversion vers segments supérieurs'
    },
    '06-Moyens Actifs': {
        'profil': 'Activité moyenne, multi-produits limitée',
        'recency': 'Moyenne',
        'valeur': 'Moyenne',
        'potentiel': 'Fidélisation'
    },
    '07-En Risque': {
        'profil': 'Anciennement actifs, engagement décroissant',
        'recency': 'Moyenne à élevée (15-45 jours)',
        'valeur': 'En déclin',
        'potentiel': 'Rétention urgente'
    },
    '08-À Réactiver': {
        'profil': 'Clients peu actifs, faible fréquence récente',
        'recency': 'Élevée (30-60 jours)',
        'valeur': 'Faible',
        'potentiel': 'Reactivation possible'
    },
    '09-Dormants': {
        'profil': 'Inactifs depuis longtemps, faible engagement',
        'recency': 'Très élevée (>60 jours)',
        'valeur': 'Très faible',
        'potentiel': 'Réactivation difficile'
    },
    '10-Perdus': {
        'profil': 'Clients inactifs, faible volume et fréquence',
        'recency': 'Extrêmement élevée',
        'valeur': 'Minime',
        'potentiel': 'Probablement irréversible'
    }
}

# Création du DataFrame stylisé
segment_df = pd.DataFrame.from_dict(segment_profiles, orient='index')

def color_cells(val):
    if 'Faible' in str(val) or 'Minime' in str(val) or 'difficile' in str(val) or 'déclin' in str(val):
        return 'background-color: #e6b8b8; color: #8b0000; font-weight: bold;'
    elif 'Moyenne' in str(val) or 'stable' in str(val) or 'Prévisible' in str(val):
        return 'background-color: #e6d6b8; color: #7d6608; font-weight: bold;'
    elif 'Élevée' in str(val) or 'Très élevée' in str(val) or 'croissante' in str(val):
        return 'background-color: #c8e6c9; color: #2e7d32; font-weight: bold;'
    elif 'Exceptionnelle' in str(val) or 'ultra-premium' in str(val):
        return 'background-color: #bbdefb; color: #1565c0; font-weight: bold;'
    else:
        return 'background-color: #f5f5f5; color: #424242;'

# Affichage du tableau stylisé
styled_table = segment_df.style \
    .applymap(color_cells) \
    .set_properties(**{
        'border': '2px solid #616161',
        'padding': '10px',
        'text-align': 'left',
        'font-family': 'Arial, sans-serif',
        'font-size': '12px'
    }) \
    .set_table_styles([{
        'selector': 'th',
        'props': [
            ('background-color', '#37474f'),
            ('color', 'white'),
            ('font-weight', 'bold'),
            ('padding', '12px'),
            ('text-align', 'center'),
            ('border', '2px solid #455a64'),
            ('font-size', '13px'),
            ('text-transform', 'uppercase')
        ]
    }, {
        'selector': 'tr:hover',
        'props': [('background-color', '#cfd8dc')]
    }]) \
    .set_caption('🎯 SEGMENTS RFM DJAMO - PROFILS DÉTAILLÉS')

display(styled_table)

# --- Statistiques clés par segment à partir de df ---
segment_analysis = df.groupby('segment_name').agg(
    recency_mean=('days_since_last_transaction', 'mean'),
    recency_median=('days_since_last_transaction', 'median'),
    frequency_mean=('txn_count_90d', 'mean'),
    frequency_median=('txn_count_90d', 'median'),
    monetary_mean=('total_txn_volume_90d', 'mean'),
    monetary_median=('total_txn_volume_90d', 'median'),
    n_clients=('clientid', 'count')
).reset_index()

segment_analysis['pct_client'] = 100 * segment_analysis['n_clients'] / segment_analysis['n_clients'].sum()

# Affichage
segment_analysis.sort_values('recency_mean').style.background_gradient(cmap='YlGnBu')
import matplotlib.pyplot as plt 
import numpy as np

# --- Calcul du pourcentage de volume ---
cluster_summary['pct_volume'] = cluster_summary['monetary_mean'] * cluster_summary['nb_clients']
cluster_summary['pct_volume'] = 100 * cluster_summary['pct_volume'] / cluster_summary['pct_volume'].sum()

# --- Ordre des segments ---
segment_order = cluster_summary['cluster_name'].tolist()

# --- Visualisations ---
fig, axes = plt.subplots(2, 2, figsize=(16, 12))
fig.suptitle('ANALYSE VISUELLE DES SEGMENTS RFM', fontsize=16, fontweight='bold')

# 1️ Bar chart du nombre de clients
bars = axes[0, 0].bar(cluster_summary['cluster_name'], cluster_summary['nb_clients'], color='skyblue')
axes[0, 0].set_title("Nombre de clients par segment", fontsize=12, fontweight='bold')
axes[0, 0].set_ylabel("Nombre de clients")
axes[0, 0].tick_params(axis="x", rotation=45)
for bar in bars:
    height = bar.get_height()
    axes[0, 0].text(bar.get_x() + bar.get_width()/2, height, f'{int(height):,}', ha='center', va='bottom', fontsize=9)

# 2️ Pie chart de la valeur totale par segment
axes[0, 1].pie(cluster_summary['pct_volume'], labels=cluster_summary['cluster_name'], autopct='%1.1f%%', startangle=140, colors=plt.cm.tab20.colors)
axes[0, 1].set_title('Répartition de la Valeur (Volume Total)', fontsize=12, fontweight='bold')

# 3️ Radar chart normalisé RFM
rfm_agg = cluster_summary.set_index('cluster_name')[['recency_median','frequency_median','monetary_median']]
rfm_norm = rfm_agg.apply(lambda x: (x - x.min()) / (x.max() - x.min()))
angles = np.linspace(0, 2*np.pi, len(rfm_norm.columns), endpoint=False).tolist()
angles += angles[:1]

for seg in rfm_norm.index:
    vals = rfm_norm.loc[seg].tolist()
    vals += vals[:1]
    axes[1, 0].plot(angles, vals, 'o-', label=seg)
    axes[1, 0].fill(angles, vals, alpha=0.1)

axes[1, 0].set_xticks(angles[:-1])
axes[1, 0].set_xticklabels(['Récence','Fréquence','Valeur'])
axes[1, 0].set_title("Radar chart des profils RFM")
axes[1, 0].legend(bbox_to_anchor=(1.2,1), fontsize=8)

# 3️ % Clients vs % Volume
x = np.arange(len(cluster_summary))
axes[1, 1].bar(x - 0.2, cluster_summary['pct_client'], width=0.4, label='% Clients', color='orange')
axes[1, 1].bar(x + 0.2, cluster_summary['pct_volume'], width=0.4, label='% Volume', color='green')
axes[1, 1].set_xticks(x)
axes[1, 1].set_xticklabels(segment_order, rotation=45, ha='right')
axes[1, 1].legend()
axes[1, 1].set_title("Comparaison % Clients vs % Volume")

plt.tight_layout()
plt.show()


# --- Tableau statistique clé ---
key_stats = cluster_summary[['cluster_name','nb_clients','pct_client','pct_volume','recency_mean','frequency_mean','monetary_mean']].copy()
display(key_stats.style.background_gradient(subset=['pct_client','pct_volume'], cmap='YlOrRd').format({
    'nb_clients':'{:,}',
    'pct_client':'{:.1f}%',
    'pct_volume':'{:.1f}%',
    'recency_mean':'{:.1f}',
    'frequency_mean':'{:.1f}',
    'monetary_mean':'{:,.0f}'
}))

#*************************

# --- Préparer les données pour le heatmap ---
heatmap_data = cluster_summary.set_index('cluster_name')[
    ['recency_mean', 'frequency_mean', 'monetary_mean']
].loc[segment_order]

# Conversion explicite en float
heatmap_data = heatmap_data.astype(float)

# --- Heatmap ---
plt.figure(figsize=(10,6))
sns.heatmap(
    heatmap_data,
    annot=True,
    fmt=".1f",
    cmap="YlGnBu",
    cbar_kws={'label':'Valeur moyenne'}
)
plt.title("Heatmap des moyennes R-F-M par segment")
plt.ylabel("Segment")
plt.show()