# Projet Big Data - Sujet 5 : Partitionnement HDFS

## Auteur
 Fatma Idoumou El Hadj C22278
 Fatiméta Issa sow C21304

Master IA_MLDS 2025-2026 – Université de Nouakchott

## Objectif
Optimiser les requêtes sur HDFS via le partitionnement hiérarchique.

## Résultats

| Format | Temps | Taille lue |
|--------|-------|------------|
| CSV Brut | 3.0 s | 455.9 Mo |
| Parquet Plat | 0.4 s | 121.6 Mo |
| Parquet Partitionné | 0.3 s | 23.9 Mo |

**Gain :** 10x plus rapide / 19x moins de données lues

## Technologies
- HDFS (NameNode + 3 DataNodes)
- Apache Spark
- Docker
- Jupyter Lab

## Fichiers
- `Rapport_Projet_Big_Data.pdf` – Rapport complet
- `benchmark_partitionnement.ipynb` – Code PySpark
- `docker-compose.yml` – Configuration du cluster
- `compose.env` – Variables d'environnement