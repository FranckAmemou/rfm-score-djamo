import pandas as pd
import numpy as np

df = df_rfm_90d.copy()

# --- R Score (inverse de la récence) ---
df['r_score'] = pd.qcut(df['days_since_last_transaction'], 5, labels=[5,4,3,2,1]).astype(int)

# --- F Score ---
df['f_score'] = pd.qcut(df['txn_count_90d'], 5, labels=[1,2,3,4,5]).astype(int)

# --- M Score ---
df['m_score'] = pd.qcut(df['total_txn_volume_90d'], 5, labels=[1,2,3,4,5]).astype(int)

# --- RFM Score ---
df['rfm_score'] = df['r_score'] + df['f_score'] + df['m_score']

# --- Vérification ---
rfm_classic = df[['clientid','r_score','f_score','m_score','rfm_score']]
rfm_classic.head()