# ===========================================
# Extraction des données RFM 90 jours - Djamo Fintech
# ===========================================

import pandas as pd
from google.cloud import bigquery
import yaml
import os

def load_config():
    """Charge la configuration depuis le fichier YAML"""
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'parameters.yaml')
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)

def extract_rfm_data():
    """
    Extrait les données RFM depuis BigQuery
    """
    config = load_config()
    
    query = f"""
    WITH snapshot_date_cte AS (
        SELECT DATE('2025-08-30') AS snapshot_date
    ),

    eligible_clients AS (
        SELECT DISTINCT clientId AS clientid
        FROM `djamo-data.production_civ_djamo.public_account`
        WHERE DATE(createdAt) <= DATE_SUB((SELECT snapshot_date FROM snapshot_date_cte), INTERVAL 90 DAY)
    ),

    active_clients_90d AS (
        SELECT DISTINCT clientid
        FROM `djamo-data.marts_growth.int_monthly_active_users`
        WHERE DATE(issueddate) BETWEEN DATE_SUB((SELECT snapshot_date FROM snapshot_date_cte), INTERVAL 90 DAY)
                                AND (SELECT snapshot_date FROM snapshot_date_cte)
        AND amount != 0
    ),

    active_client AS (
        SELECT DISTINCT e.clientid
        FROM eligible_clients e
        INNER JOIN active_clients_90d a USING (clientid)
    ),

    last_txn AS (
        SELECT
            m.clientid,
            DATE_DIFF((SELECT snapshot_date FROM snapshot_date_cte), MAX(DATE(m.issueddate)), DAY) AS days_since_last_transaction
        FROM `djamo-data.marts_growth.int_monthly_active_users` m
        WHERE DATE(m.issueddate) <= (SELECT snapshot_date FROM snapshot_date_cte)
        AND m.clientid IN (SELECT clientid FROM active_client)
        GROUP BY m.clientid
    ),

    txn_90d AS (
        SELECT
            m.clientid,
            COUNT(*) AS txn_count_90d,
            COUNT(DISTINCT DATE(m.issueddate)) AS active_days_count_90d,
            SUM(ABS(m.amount)) AS total_txn_volume_90d,
            AVG(ABS(m.amount)) AS avg_total_txn_volume_90d,
            STDDEV_POP(ABS(m.amount)) AS value_consistency_std
        FROM `djamo-data.marts_growth.int_monthly_active_users` m
        WHERE DATE(m.issueddate) BETWEEN DATE_SUB((SELECT snapshot_date FROM snapshot_date_cte), INTERVAL 90 DAY)
                                    AND (SELECT snapshot_date FROM snapshot_date_cte)
        AND m.clientid IN (SELECT clientid FROM active_client)
        GROUP BY m.clientid
    ),

    current_balance_snapshot AS (
        WITH ranked_balances AS (
            SELECT
                clientid,
                balance,
                updatedAt,
                ROW_NUMBER() OVER (PARTITION BY clientid ORDER BY updatedAt DESC) as rn
            FROM `djamo-data.production_civ_djamo.public_account`
            WHERE category = "primary"
            AND DATE(updatedAt) <= (SELECT snapshot_date FROM snapshot_date_cte)
            AND clientid IN (SELECT clientid FROM active_client)
        )
        SELECT clientid, balance AS current_account_balance_snapshot
        FROM ranked_balances
        WHERE rn = 1
    ),

    vault_balance_snapshot AS (
        WITH ranked_vault_balances AS (
            SELECT
                clientid,
                balance,
                updatedAt,
                ROW_NUMBER() OVER (PARTITION BY clientid ORDER BY updatedAt DESC) as rn
            FROM `djamo-data.production_civ_djamo.public_account`
            WHERE category = "vault"
            AND DATE(updatedAt) <= (SELECT snapshot_date FROM snapshot_date_cte)
            AND clientid IN (SELECT clientid FROM active_client)
        )
        SELECT clientid, balance AS vault_balance_snapshot
        FROM ranked_vault_balances
        WHERE rn = 1
    ),

    aum_snapshot AS (
        SELECT
            acc.clientId AS clientid,
            SUM(CAST(s.amount AS FLOAT64)) AS aum_snapshot
        FROM `djamo-data.production_civ_djamo.nsia_invest_account` acc
        JOIN `djamo-data.production_civ_djamo.nsia_invest_subscription` s
            ON acc.id = s.accountId
        WHERE acc.isActive = TRUE
            AND acc.deletedAt IS NULL
            AND s.status = 'validated'
            AND acc.clientId IN (SELECT clientid FROM active_client)
            AND DATE(s.updatedAt) <= (SELECT snapshot_date FROM snapshot_date_cte)
        GROUP BY acc.clientId
    ),

    has_vault AS (
        SELECT DISTINCT clientid, 1 AS has_vault
        FROM `djamo-data.production_civ_djamo.public_account`
        WHERE category = "vault"
        AND clientid IN (SELECT clientid FROM active_client)
    ),

    has_invest AS (
        SELECT DISTINCT clientId AS clientid, 1 AS has_invest
        FROM `djamo-data.production_civ_djamo.nsia_invest_account`
        WHERE isActive = TRUE
        AND deletedAt IS NULL
        AND clientId IN (SELECT clientid FROM active_client)
    )

    SELECT 
        ac.clientid,
        (SELECT snapshot_date FROM snapshot_date_cte) AS snapshot_date,
        COALESCE(l.days_since_last_transaction, 90) AS days_since_last_transaction,
        COALESCE(t.txn_count_90d, 0) AS txn_count_90d,
        COALESCE(t.total_txn_volume_90d, 0) AS total_txn_volume_90d,
        COALESCE(t.active_days_count_90d, 0) AS active_days_count_90d,
        1 + COALESCE(vh.has_vault, 0) + COALESCE(ih.has_invest, 0) AS nb_active_products,
        COALESCE(t.value_consistency_std, 0) AS value_consistency_std,
        SAFE_DIVIDE(COALESCE(t.txn_count_90d, 0), GREATEST(COALESCE(t.active_days_count_90d, 0), 1)) AS txn_frequency_ratio,
        COALESCE(t.avg_total_txn_volume_90d, 0) AS avg_total_txn_volume_90d,
        COALESCE(cb.current_account_balance_snapshot, 0) AS current_account_balance,
        COALESCE(vb.vault_balance_snapshot, 0) AS vault_balance,
        COALESCE(a.aum_snapshot, 0) AS aum_snapshot,
        COALESCE(cb.current_account_balance_snapshot, 0) +
        COALESCE(vb.vault_balance_snapshot, 0) +
        COALESCE(a.aum_snapshot, 0) AS total_assets_snapshot,
        SAFE_DIVIDE(COALESCE(t.total_txn_volume_90d, 0), GREATEST(COALESCE(cb.current_account_balance_snapshot, 0), 1)) AS balance_velocity_90d
    FROM active_client ac
    LEFT JOIN txn_90d t ON ac.clientid = t.clientid
    LEFT JOIN last_txn l ON ac.clientid = l.clientid
    LEFT JOIN current_balance_snapshot cb ON ac.clientid = cb.clientid
    LEFT JOIN vault_balance_snapshot vb ON ac.clientid = vb.clientid
    LEFT JOIN aum_snapshot a ON ac.clientid = a.clientid
    LEFT JOIN has_vault vh ON ac.clientid = vh.clientid
    LEFT JOIN has_invest ih ON ac.clientid = ih.clientid
    ORDER BY txn_count_90d DESC
    """

    # Exécution de la requête
    client = bigquery.Client(project='djamo-data')
    df_rfm_90d = client.query(query).to_dataframe()
    print(f"Données extraites: {df_rfm_90d.shape}")
    
    return df_rfm_90d

def save_data(df, filename="rfm_data.csv"):
    """
    Sauvegarde les données extraites
    """
    processed_dir = os.path.join(os.path.dirname(__file__), '..', 'data', 'processed')
    os.makedirs(processed_dir, exist_ok=True)
    
    filepath = os.path.join(processed_dir, filename)
    df.to_csv(filepath, index=False)
    print(f"Données sauvegardées: {filepath}")

if __name__ == "__main__":
    df = extract_rfm_data()
    print(f"Données extraites: {df.shape}")
    save_data(df)
    print(df.head())