#  Travaux Pratiques Hadoop — Solution 1

> Solutions complètes des TP HDFS, MapReduce et YARN pour un cluster Hadoop distribué.
 realise par Fatimeta issa sow
---

##  Table des matières

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

##  Technologies utilisées <a name="technologies"></a>

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
# Attendu : openjdk version "1.8.x" ou supérieur

# Hadoop (si exécution hors Docker)
hadoop version
# Attendu : Hadoop 3.2.1

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

##  Structure du dossier <a name="structure"></a>

```
solution1/
├── tp1.sh        # TP1 : Toutes les commandes HDFS (CLI + Architecture)
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
# Tous les services doivent être en état "Up"

# 4. Attendre 30-60 secondes, puis vérifier les interfaces :
# NameNode HDFS    : http://localhost:9870
# ResourceManager  : http://localhost:8088
# History Server   : http://localhost:8188
```

### Option B — Cluster Hadoop existant

```bash
# Vérifier la connectivité
hdfs dfsadmin -report
yarn node -list
```

---

##  TP1  : Stockage Distribué <a name="tp1"></a>

### Exécution complète

```bash
# Rendre le script exécutable
chmod +x tp-hadoop.sh

# Lancer tous les exercices
./tp-hadoop.sh
```

### Ce que fait le script

Le script `tp-hadoop.sh` couvre l'intégralité des exercices du TP1 :

**Exercice 1 — Navigation :**
- Création de `/user/etudiant_alpha/data/input` et `/data/out`
- Démonstration de `ls` vs `ls -R` avec explication
- Suppression du dossier `out` et explication du mécanisme `.Trash`

**Exercice 2 — Transfert de données :**
- Création d'un fichier local de 1 Mo avec `dd`
- Upload via `put` ET `copyFromLocal` (deux méthodes)
- Téléchargement avec renommage (`get`)
- Lecture sans téléchargement (`cat | head -n 10`)

**Exercice 3 — Droits et Quotas :**
- `chmod 444` → lecture seule pour tous
- `chown hdfs:supergroup` → changement de propriétaire
- `setSpaceQuota 50m` → quota de 50 Mo
-  Test de dépassement avec un fichier de 60 Mo

**Exercice 4 — Fusion :**
- `getmerge` → fusion de 3 fichiers en un seul
- Explication `cp` vs `put`
- `du -s -h` → statistiques disque

**Exercice 5 — Distribution des blocs :**
- `hdfs fsck -files -blocks -locations`
- Explication de l'occupation mémoire NameNode pour petits fichiers

**Exercice 6 — Réplication dynamique :**
- `setrep -w 2` → réduction du facteur de réplication
- Explication du délai de suppression des répliques excédentaires

**Exercice 7 — Tolérance aux pannes :**
-  Arrêt simulé d'un DataNode (commande Docker)
- `dfsadmin -report` → état du cluster
- Explication du délai de détection (~10 minutes)

**Exercice 8 — Safe Mode :**
-  `safemode enter` → activation forcée
- Test de suppression en Safe Mode (doit échouer)
- `safemode leave` → désactivation
- `hdfs fsck / -summary` → vérification d'intégrité

---

## 🗺 TP2 — MapReduce : Traitement Distribué <a name="tp2"></a>

### Job 1 : Comptage des erreurs HTTP (`ErrorCount.java`)

**Compilation et exécution manuelle :**

```bash
# Compilation
HADOOP_CP=$(hadoop classpath)
mkdir -p /tmp/classes_erreurs
javac -classpath "${HADOOP_CP}" -d /tmp/classes_erreurs ErrorCount.java

# Création du JAR
jar -cvf ErrorCount.jar -C /tmp/classes_erreurs .

# Chargement des données de test sur HDFS
hdfs dfs -mkdir -p /user/etudiant_alpha/logs/input
echo "2024-01-15 | 10.0.0.5 | /api | 404 | 512" | \
  hdfs dfs -put - /user/etudiant_alpha/logs/input/test.log

# Lancement du job
hadoop jar ErrorCount.jar ErrorCount \
    /user/etudiant_alpha/logs/input \
    /user/etudiant_alpha/logs/output

# Résultats
hdfs dfs -cat /user/etudiant_alpha/logs/output/part-r-00000
```

**Sortie attendue :**
```
401     1
403     2
404     4
500     3
```

### Job 2 : Amis communs (`FriendsCommon.java`)

**Format d'entrée (fichier texte) :**
```
A    B,C,D
B    A,C,E
C    A,B,D
D    A,C
E    B
```

**Sortie attendue :**
```
(A,B)    [C]
(A,C)    [B, D]
(A,D)    [C]
(B,C)    [A]
```

### Exécution automatisée

```bash
chmod +x mapreduce.sh
./mapreduce.sh
# Le script compile, uploade les données, lance les deux jobs et affiche les résultats.
```

### Observer les compteurs Shuffle sur YARN

```bash
# Après l'exécution, récupérez l'APP_ID et consultez les compteurs
yarn logs -applicationId application_XXXXX_0001 | grep -E "Shuffle|Bytes"
# Cherchez : "SHUFFLE_BYTES" dans les compteurs du job
```

---

##  TP3 — YARN : Gestion des Ressources <a name="tp3"></a>

### Démarrage du cluster

```bash
# Configuration Solution 1 : 4096 Mo total, 2048 Mo max par container
docker-compose -f yarn.yml up -d

# Vérification de la capacité (doit afficher 4096 Mo)
curl -s http://localhost:8088/ws/v1/cluster/metrics | python3 -c \
  "import sys,json; m=json.load(sys.stdin)['clusterMetrics']; print(f'Mémoire : {m[\"totalMB\"]} Mo')"
```

### Commandes de monitoring

```bash
chmod +x yarn.sh
./yarn.sh
```

Le script `yarn.sh` démontre :

```bash
# Lister les applications
yarn application -list
yarn application -list -appStates FINISHED,FAILED

# Arrêter une application ⚠️
yarn application -kill application_1234567890_0001

# Consulter les logs
yarn logs -applicationId application_1234567890_0001

# État des nœuds
yarn node -list -all

# État des queues
yarn queue -status default
```

### Test de saturation des ressources

```bash
# Lancer deux jobs simultanément pour observer la mise en attente (Pending)
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar \
    pi 50 1000 &

hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar \
    pi 50 1000 &

# Observer sur http://localhost:8088 : le second job passe en ACCEPTED/PENDING
```

### Modifier la mémoire à chaud

```bash
# Modifier yarn.yml : changer 4096 → 2048
# Puis redémarrer le NodeManager uniquement
docker-compose -f yarn.yml restart nodemanager

# Observer l'impact sur http://localhost:8088 (capacité réduite à 2048 Mo)
```

---

##  Exemples de sorties attendues <a name="exemples"></a>

### HDFS — `hdfs dfsadmin -report`

```
Configured Capacity: 107374182400 (100 GB)
Present Capacity: 95000000000 (88.49 GB)
DFS Remaining: 80000000000 (74.51 GB)
DFS Used: 15000000000 (13.97 GB)
DFS Used%: 13.97%
Live datanodes (1):
  Name: 172.20.0.3:9866
  ...
Dead datanodes (0):
```

### MapReduce — ErrorCount

```
401     3
403     8
404     42
500     17
503     5
```

### YARN — `yarn application -list`

```
Total number of applications (application-types: [] and states: [RUNNING]):1
Application-Id          Application-Name  State   Progress
application_1700000000_0001  Word Count  RUNNING  50%
```

---

##  Erreurs fréquentes et solutions <a name="erreurs"></a>

| Erreur | Cause probable | Solution |
|--------|---------------|----------|
| `Connection refused (localhost:9000)` | NameNode non démarré | `docker-compose -f yarn.yml up -d` |
| `NSQuotaExceededException` | Quota HDFS dépassé | `hdfs dfsadmin -clrSpaceQuota /user/...` |
| `SafeModeException` | NameNode en Safe Mode | `hdfs dfsadmin -safemode leave` |
| `ClassNotFoundException` | JAR mal créé | Vérifier `jar tf ErrorCount.jar` |
| `OutOfMemoryError` dans container | max-allocation trop bas | Augmenter `yarn.scheduler.maximum-allocation-mb` |
| `Shuffle hang` | Data skew (célébrité) | Vérifier le seuil `SEUIL_CELEBRITE` dans `FriendsCommon.java` |
| `Permission denied` | Droits HDFS insuffisants | `hdfs dfs -chmod 755 /user/<nom>` |
| `No space left on device` | Volume Docker plein | `docker system prune -f` |

