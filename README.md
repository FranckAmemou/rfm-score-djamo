
# RFM Segmentation Project

##  Description
Ce projet analyse les comportements clients via une **segmentation RFM (Recency, Frequency, Monetary)**.  
Il permet de classer les clients en 10 segments marketing selon leur récence, fréquence et montant de transaction.

- **R (Recency)** : Nombre de jours depuis la dernière transaction
- **F (Frequency)** : Nombre de transactions sur les 90 derniers jours
- **M (Monetary)** : Volume total des transactions sur les 90 derniers jours

La segmentation est **rule-based** (basée sur des seuils métiers) et produit un résumé descriptif des segments.

---

## Structure du projet
rfm-churn-analysis/
│── data/                  
│   ├── raw/               
│   └── processed/         
│
│── notebooks/             
│   ├── 01_data_exploration.ipynb
│   └── 02_rfm_segmentation.ipynb
│
│── src/                   
│   ├── __init__.py
│   ├── data_extraction.py          # Extraction population + données RFM depuis BigQuery
│   ├── rfm_segmentation.py         # Calcul scores RFM et attribution segments
│   └── utils.py                    # Fonctions utilitaires (plots, métriques)
│               
│       
│
│── reports/               
│   ├── segment_df_real.csv
│   └── figures/
│       └── rfm_distribution.png
│
│── tests/                 
│   └── test_rfm_segmentation.py
│
│── .gitignore             
│── requirements.txt       
│── README.md              
│── LICENSE                

