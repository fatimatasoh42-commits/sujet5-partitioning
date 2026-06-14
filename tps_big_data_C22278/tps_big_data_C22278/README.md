```markdown
# 🐘 Travaux Pratiques Hadoop — Solution 1

> Solutions complètes des TP HDFS, MapReduce et YARN pour un cluster Hadoop distribué.

---

## 📑 Table des matières

1. [Description du projet](#description)
2. [Technologies utilisées](#technologies)
3. [Prérequis](#prérequis)
4. [Structure du dossier](#structure)
5. [Installation](#installation)
6. [TP1 — HDFS : Stockage Distribué](#tp1)
7. [TP2 — MapReduce : Traitement Distribué](#tp2)
8. [TP3 — YARN : Gestion des Ressources](#tp3)
9. [Exemples de sorties attendues](#exemples)
10. [Erreurs fréquentes et solutions](#erreurs)
11. [Licence](#licence)

---

##  Description du projet <a name="description"></a>

Ce dépôt contient la **Solution 1** des trois travaux pratiques sur l'écosystème Hadoop :

| TP | Thème | Objectif |
|----|-------|----------|
| TP1 | HDFS | Maîtriser le stockage distribué, les commandes CLI et la tolérance aux pannes |
| TP2 | MapReduce | Implémenter des algorithmes distribués (comptage d'erreurs, amis communs) |
| TP3 | YARN | Gérer les ressources du cluster, configurer les schedulers, monitorer les jobs |

Les scripts sont écrits en **Bash** (commentaires en français) et le code en **Java 8**.

---

## Technologies utilisées <a name="technologies"></a>

| Technologie | Version | Rôle |
|-------------|---------|------|
| Apache Hadoop | 3.2.1 | Plateforme Big Data (HDFS + MapReduce + YARN) |
| Java | 8+ | Implémentation des jobs MapReduce |
| Docker | 20.10+ | Conteneurisation du cluster |
| Docker Compose | 3.8 | Orchestration des services Hadoop |
| Bash | 5+ | Scripts d'automatisation |

---

##  Prérequis <a name="prérequis"></a>

Avant de commencer, assurez-vous d'avoir installé :

```bash
# Java 8 ou supérieur
java -version

# Hadoop (si exécution hors Docker)
hadoop version

# Docker et Docker Compose
docker --version
docker-compose --version

# Git
git --version
```

**Variables d'environnement nécessaires :**

```bash
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
```

---

## Structure du dossier <a name="structure"></a>

```
solution1/
├── tp-hadoop.sh        # TP1 : Toutes les commandes HDFS (CLI + Architecture)
├── ErrorCount.java     # TP2 : Job MapReduce — Comptage des erreurs HTTP
├── FriendsCommon.java  # TP2 : Job MapReduce — Amis communs dans un réseau social
├── mapreduce.sh        # TP2 : Compilation et exécution des deux jobs Java
├── yarn.yml            # TP3 : Docker-Compose du cluster YARN (4096 Mo / 2048 Mo max)
├── yarn.sh             # TP3 : Commandes de monitoring et gestion YARN
└── README.md           # Ce fichier
```

---

##  Installation <a name="installation"></a>

### Option A — Cluster Docker (recommandé pour les TP)

```bash
# 1. Cloner le dépôt
git clone <url-du-depot>
cd solution1

# 2. Démarrer le cluster Hadoop complet
docker-compose -f yarn.yml up -d

# 3. Vérifier que tous les services sont démarrés
docker-compose -f yarn.yml ps

# 4. Attendre 30-60 secondes, puis vérifier les interfaces :
# ResourceManager : http://localhost:8088
```

### Option B — Cluster Hadoop existant

```bash
# Vérifier la connectivité
hdfs dfsadmin -report
yarn node -list
```

---

##  TP1 — HDFS : Stockage Distribué <a name="tp1"></a>

### Exécution complète

```bash
# Rendre le script exécutable
chmod +x tp-hadoop.sh

# Lancer tous les exercices
./tp-hadoop.sh
```

### Ce que fait le script `tp-hadoop.sh`

Le script couvre l'intégralité des exercices du TP1 :

| Exercice | Description | Commandes utilisées |
|----------|-------------|---------------------|
| 1 | Navigation et gestion des dossiers | `mkdir`, `ls`, `ls -R`, `rm`, `.Trash` |
| 2 | Transfert de données | `put`, `copyFromLocal`, `get`, `cat` |
| 3 | Gestion des droits et quotas | `chmod`, `chown`, `setSpaceQuota` |
| 4 | Fusion et archivage | `getmerge`, `cp`, `du` |
| 5 | Analyse des blocs | `hdfs fsck -files -blocks -locations` |
| 6 | Facteur de réplication | `setrep` |
| 7 | Tolérance aux pannes | `dfsadmin -report` (simulation) |
| 8 | Mode Safemode | `safemode enter/leave` |

### Commandes dangereuses dans le TP1

| Commande | Risque | Protection |
|----------|--------|------------|
| `hdfs dfs -rm -r` | Suppression définitive | Vérifier le chemin 2 fois |
| `hdfs dfsadmin -setSpaceQuota` | Bloque les écritures si dépassé | Tester avec un petit fichier d'abord |
| `hdfs dfsadmin -safemode enter` | Bloque toutes les écritures | Sortir rapidement avec `leave` |

---

##  TP2 — MapReduce : Traitement Distribué <a name="tp2"></a>

### Job 1 : Comptage des erreurs HTTP (`ErrorCount.java`)

**Objectif :** Compter le nombre d'occurrences de chaque code d'erreur (404, 500, etc.)

**Format d'entrée :**
```
DATE | IP | URL | STATUS | SIZE
2024-01-01 | 192.168.1.1 | /home | 200 | 1024
2024-01-01 | 192.168.1.2 | /login | 404 | 512
```

**Format de sortie :**
```
STATUS    count
404       2
500       1
```

### Job 2 : Amis communs (`FriendsCommon.java`)

**Objectif :** Trouver les amis communs entre chaque paire d'utilisateurs

**Format d'entrée :**
```
A -> B,C,D
B -> A,C
C -> A,B,E
```

**Format de sortie :**
```
(A,B)    [C]
(A,C)    [B]
(B,C)    [A]
```

### Exécution automatisée

```bash
chmod +x mapreduce.sh
./mapreduce.sh
# Le script compile, uploade les données, lance les deux jobs et affiche les résultats
```

### Observer les compteurs Shuffle sur YARN

```bash
# Après l'exécution, récupérez l'APP_ID et consultez les compteurs
yarn logs -applicationId application_XXXXX_0001 | grep -E "Shuffle|Bytes"
```

---

##  TP3 — YARN : Gestion des Ressources <a name="tp3"></a>

### Démarrage du cluster

```bash
# Configuration Solution 1 : 4096 Mo total, 2048 Mo max par container
docker-compose -f docker_compose.yml up -d

# Vérification de la capacité
curl -s http://localhost:8088/ws/v1/cluster/metrics | python3 -c \
  "import sys,json; m=json.load(sys.stdin)['clusterMetrics']; print(f'Mémoire : {m[\"totalMB\"]} Mo')"
```

### Commandes de monitoring (`yarn.sh`)

```bash
chmod +x tp3.sh
./yarn.sh
```

Le script `tp3.sh` contient :

```bash
# Lister les applications
yarn application -list

# Arrêter une application 
yarn application -kill application_1234567890_0001

# Consulter les logs
yarn logs -applicationId application_1234567890_0001

# État des nœuds
yarn node -list -all

# État des queues
yarn queue -status default
```

### Accès à l'interface Web

```
http://localhost:8088
```

### Test de saturation des ressources

```bash
# Lancer deux jobs simultanément
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar pi 50 1000 &
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar pi 50 1000 &

# Observer sur http://localhost:8088 : le second job passe en ACCEPTED/PENDING
```

### Modifier la mémoire à chaud

```bash
# Modifier yarn.yml : changer 4096 → 2048
# Puis redémarrer le NodeManager
docker-compose -f yarn.yml restart nodemanager
```

### Arrêter le cluster

```bash
docker-compose -f yarn.yml down
```

---

## Exemples de sorties attendues <a name="exemples"></a>

### TP1 — `hdfs dfsadmin -report`

```
Live datanodes (1):
  Name: 172.20.0.3:9866
  DFS Used: 15 GB (13.97%)
Dead datanodes (0):
```

### TP2 — `ErrorCount`

```
401     3
403     8
404     42
500     17
```

### TP2 — `FriendsCommon`

```
(A,B)    [C]
(A,C)    [B, D]
(A,D)    [C]
(B,C)    [A]
```

### TP3 — `yarn application -list`

```
Application-Id          Application-Name  State   Progress
application_1700000000_0001  Word Count  RUNNING  50%
```

---

##  Erreurs fréquentes et solutions <a name="erreurs"></a>

| Erreur | Cause probable | Solution |
|--------|---------------|----------|
| `Connection refused` | NameNode non démarré | `docker-compose -f yarn.yml up -d` |
| `Quota exceeded` | Quota HDFS dépassé | `hdfs dfsadmin -clrSpaceQuota /user/...` |
| `SafeModeException` | NameNode en Safe Mode | `hdfs dfsadmin -safemode leave` |
| `ClassNotFoundException` | JAR mal créé | Vérifier `jar tf ErrorCount.jar` |
| `OutOfMemoryError` | max-allocation trop bas | Augmenter `yarn.scheduler.maximum-allocation-mb` |
| `Permission denied` | Droits HDFS insuffisants | `hdfs dfs -chmod 755 /user/<nom>` |

