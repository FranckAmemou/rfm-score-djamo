

import pandas as pd
from data_extraction import extract_rfm_data  # importer la fonction

df_rfm_90d = extract_rfm_data()
df_rfm = df_rfm_90d.copy()

# --- Colonnes RFM ---
R = 'days_since_last_transaction'
F = 'txn_count_90d'
M = 'total_txn_volume_90d'

# --- Attribution des scores R, F, M selon seuils business ---
def score_recency(x):
    if x <= 7: return 5
    elif x <= 14: return 4
    elif x <= 30: return 3
    elif x <= 60: return 2
    else: return 1

def score_frequency(x):
    if x >= 150: return 5
    elif x >= 50: return 4
    elif x >= 20: return 3
    elif x >= 5: return 2
    else: return 1

def score_monetary(x):
    if x >= 1_000_000: return 5
    elif x >= 100_000: return 4
    elif x >= 10_000: return 3
    elif x >= 1_000: return 2
    else: return 1

df_rfm['R_score'] = df_rfm[R].apply(score_recency)
df_rfm['F_score'] = df_rfm[F].apply(score_frequency)
df_rfm['M_score'] = df_rfm[M].apply(score_monetary)

# --- Score RFM numérique pour tri ---
df_rfm['RFM_score'] = df_rfm['R_score'].astype(str) + df_rfm['F_score'].astype(str) + df_rfm['M_score'].astype(str)
df_rfm['RFM_score_num'] = df_rfm[['R_score','F_score','M_score']].sum(axis=1)

# --- Attribution de segments marketing ---
def rfm_10_segments_business(row):
    R = row['R_score']
    F = row['F_score']
    M = row['M_score']

    if R == 5 and F == 5 and M == 5:
        return 'Champions'
    elif R >= 4 and F >= 4:
        return 'Fidèles Premium'
    elif R >= 4 and M >= 4:
        return 'Gros dépensiers occasionnels'
    elif R == 5 and F <= 2 and M <= 2:
        return 'Nouveaux Clients'
    elif R >= 3 and F >= 3 and M >= 3:
        return 'Réguliers Stables'
    elif R <= 2 and F <= 2 and M <= 2:
        return 'Clients Perdus / Inactifs'
    elif R <= 2 and M >= 4:
        return 'Anciens Gros Clients'
    elif R <= 3 and F >= 3 and M <= 3:
        return 'Irréguliers avec potentiel'
    elif R <= 3 and F <= 2 and M <= 3:
        return 'Hésitants / Peu actifs'
    else:
        return 'Nouveaux à activer'

df_rfm['Segment'] = df_rfm.apply(rfm_10_segments_business, axis=1)

# --- Résumé par segment sans RFM_score et sans RFM_score_num affichés ---
summary = df_rfm.groupby('Segment').agg(
    nb_clients=('clientid','count'),
    recency_min=(R,'min'),
    recency_mean=(R,'mean'),
    recency_max=(R,'max'),
    frequency_min=(F,'min'),
    frequency_mean=(F,'mean'),
    frequency_max=(F,'max'),
    monetary_min=(M,'min'),
    monetary_mean=(M,'mean'),
    monetary_max=(M,'max')
).reset_index()

# --- Pourcentage clients ---
summary['pct_clients'] = (summary['nb_clients'] / df_rfm.shape[0] * 100).round(2)

print(df_rfm[['clientid','R_score','F_score','M_score','RFM_score','RFM_score_num','Segment']].head(10))

# --- Trier par RFM_score_num sans l’afficher ---
summary = summary.merge(
    df_rfm.groupby('Segment')['RFM_score_num'].mean().reset_index(),
    on='Segment'
).sort_values('RFM_score_num', ascending=False).drop(columns='RFM_score_num')

# --- Affichage final ---
print(summary)
summary.to_csv('../reports/segment_df_real.csv', index=False)