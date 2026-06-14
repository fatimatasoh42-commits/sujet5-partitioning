# Projet Big Data - Sujet 5 : Partitionnement HDFS
Master IA - Universite de Nouakchott
Encadre par : Dr. EL BENANY Med Mahmoud
Juin 2026

---

## Auteurs
- Fatimata Issa Sow
- Fatma Idoumou El Hadj

---

## 1. Objectif
Optimiser les requetes sur HDFS en organisant physiquement 
les donnees par partitionnement hierarchique.

---

## 2. Architecture (deux clusters separes)

Notre infrastructure comprend deux clusters independants 
relies par le nom du conteneur namenode :

| Cluster | Role | Composants | Reseau |
|---------|------|------------|--------|
| HDFS | Stockage distribue | 1 NameNode + 3 DataNodes | hdfs_network |
| Spark | Calcul distribue | 1 Master + 2 Workers + Jupyter | spark_network |

Spark accede a HDFS via l'adresse hdfs://namenode:9000
configuree dans CORE_CONF_fs_defaultFS.

---

## 3. Technologies
- Docker / Docker Compose
- Hadoop HDFS 3.2.1
- Apache Spark 3.3.0
- PySpark
- Jupyter Lab

---

## 4. Structure du depot

BigData_Project/
    docker-compose.yml
    compose.env
    benchmark_partitionnement.ipynb
    README.md
    Rapport_Projet_Big_Data.pdf

---

## 5. Execution

### 5.1 Demarrer le cluster
docker-compose --env-file compose.env up -d

### 5.2 Upload du CSV vers HDFS
docker exec -it namenode bash
hdfs dfs -put /data/2023_Yellow_Taxi_Trip_Data.csv /

### 5.3 Acces aux interfaces

| Interface | URL |
|-----------|-----|
| Jupyter Lab | http://localhost:8888 |
| Spark UI | http://localhost:4040 |
| NameNode UI | http://localhost:9870 |
| Spark Master | http://localhost:8080 |
| Worker 1 | http://localhost:8081 |
| Worker 2 | http://localhost:8082 |

### 5.4 Executer le notebook
Ouvrir benchmark_partitionnement.ipynb dans Jupyter 
et executer toutes les cellules.

---

## 6. Resultats

Requete : SELECT COUNT(*) FROM taxi WHERE VendorID = 1 AND Month = 1

| Format | Temps | Taille lue |
|--------|-------|------------|
| CSV Brut | 3.0 s | 455.9 Mo |
| Parquet Plat | 0.4 s | 121.6 Mo |
| Parquet Partitionne | 0.3 s | 23.9 Mo |

Gain : 10x plus rapide et 19x moins de donnees lues.

---

## 7. Concepts cles

**Partition Pruning** : Spark lit uniquement le dossier 
VendorID=1/Month=1/ au lieu de tout scanner.

**Over-partitioning** : 10 000 fichiers de 10 Ko = 
saturation RAM du NameNode. Probleme critique a grande echelle.

**Taille de bloc HDFS** : Bloc par defaut = 128 Mo. 
Notre partition = 23.9 Mo. Ideal = entre 128 Mo et 256 Mo.

---

## 8. Application a la Mauritanie

| Secteur | Partitionnement | Benefice |
|---------|----------------|----------|
| Recensement national | Wilaya / Moughataa | Interroger une commune sans scanner tout le pays |
| SMELEC | Region / Mois | Analyser Nouadhibou sans lire Nouakchott |
| SNIM | Type de machine | Diagnostic instantane sur une locomotive |

---

## 9. Conclusion

Le partitionnement physique sur HDFS accelere les requetes 
par un facteur 10 et reduit de 19 fois le volume de donnees lues. 
Technique essentielle pour les infrastructures nationales.
