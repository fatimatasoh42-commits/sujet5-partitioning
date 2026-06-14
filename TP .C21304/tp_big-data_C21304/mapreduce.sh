
# ─── Paramètres configurables ─────────────────────────────────────────────────
HADOOP_HOME="${HADOOP_HOME:-/opt/hadoop}"
HADOOP_JAR="${HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar"
JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-8-openjdk-amd64}"

# Chemins HDFS
HDFS_BASE="/user/etudiant_alpha"
HDFS_LOGS_INPUT="${HDFS_BASE}/mapreduce/logs/input"
HDFS_LOGS_OUTPUT="${HDFS_BASE}/mapreduce/logs/output_erreurs"
HDFS_GRAPH_INPUT="${HDFS_BASE}/mapreduce/social/input"
HDFS_GRAPH_OUTPUT="${HDFS_BASE}/mapreduce/social/output_amis"

# Répertoire de travail local
WORK_DIR="/tmp/tp2_solution1"
JAR_ERREURS="${WORK_DIR}/ErrorCount.jar"
JAR_AMIS="${WORK_DIR}/FriendsCommon.jar"



# ─── Préparation de l'environnement ──────────────────────────────────────────
echo ""
echo "[INIT] Préparation de l'environnement de travail..."
mkdir -p "${WORK_DIR}/classes_erreurs"
mkdir -p "${WORK_DIR}/classes_amis"

# Vérification de Hadoop
if ! command -v hdfs &> /dev/null; then
    echo "ERREUR : hdfs non trouvé. Vérifiez HADOOP_HOME=${HADOOP_HOME}"
    exit 1
fi

# Récupération du classpath Hadoop
HADOOP_CP=$(hadoop classpath)
echo "[INIT] Classpath Hadoop chargé."

# ─── Données de test ─────────────────────────────────────────────────────────
echo ""
echo "[DONNÉES] Création des données de test..."

# Logs serveur de test (format: DATE | IP | URL | STATUS | SIZE)
cat > "${WORK_DIR}/logs_test.txt" << 'LOGS'
2024-01-15 | 192.168.1.10 | /index.html | 200 | 2048
2024-01-15 | 10.0.0.5     | /api/users  | 404 | 512
2024-01-15 | 172.16.0.3   | /login      | 500 | 256
2024-01-15 | 192.168.1.20 | /images/bg  | 404 | 0
2024-01-15 | 10.0.0.7     | /api/data   | 200 | 4096
2024-01-15 | 192.168.2.1  | /admin      | 403 | 128
2024-01-15 | 10.0.0.5     | /api/delete | 404 | 512
2024-01-15 | 172.16.0.3   | /upload     | 500 | 0
2024-01-15 | 192.168.1.10 | /css/style  | 200 | 8192
2024-01-15 | 10.0.0.9     | /page404    | 404 | 256
2024-01-16 | 192.168.3.1  | /api/login  | 401 | 64
2024-01-16 | 10.0.0.5     | /restricted | 403 | 0
2024-01-16 | 172.16.0.5   | /crash      | 500 | 0
LOGS

echo "      ✓ Fichier logs créé ($(wc -l < ${WORK_DIR}/logs_test.txt) lignes)."

# Réseau social de test
cat > "${WORK_DIR}/social_test.txt" << 'SOCIAL'
A	B,C,D
B	A,C,E
C	A,B,D
D	A,C
E	B
SOCIAL

echo "      ✓ Fichier réseau social créé."

# Upload vers HDFS
echo ""
echo "[HDFS] Upload des données de test..."
hdfs dfs -mkdir -p "${HDFS_LOGS_INPUT}"
hdfs dfs -mkdir -p "${HDFS_GRAPH_INPUT}"
hdfs dfs -put -f "${WORK_DIR}/logs_test.txt" "${HDFS_LOGS_INPUT}/"
hdfs dfs -put -f "${WORK_DIR}/social_test.txt" "${HDFS_GRAPH_INPUT}/"
echo "      ✓ Données uploadées sur HDFS."


javac -classpath "${HADOOP_CP}" \
      -d "${WORK_DIR}/classes_erreurs" \
      "$(dirname "$0")/ErrorCount.java" 2>&1

if [ $? -ne 0 ]; then
    echo "ERREUR : Compilation de ErrorCount.java échouée."
    exit 1
fi
echo "      ✓ Compilation réussie."

echo "[JAR] Création de ErrorCount.jar..."
jar -cvf "${JAR_ERREURS}" -C "${WORK_DIR}/classes_erreurs" . > /dev/null 2>&1
echo "      ✓ JAR créé : ${JAR_ERREURS}"

# Suppression de l'ancien output HDFS si existant
hdfs dfs -rm -r -f "${HDFS_LOGS_OUTPUT}" > /dev/null 2>&1

echo ""
echo "[EXEC] Lancement du job ErrorCount..."
echo "       Input  : ${HDFS_LOGS_INPUT}"
echo "       Output : ${HDFS_LOGS_OUTPUT}"
echo ""

hadoop jar "${JAR_ERREURS}" ErrorCount \
    "${HDFS_LOGS_INPUT}" \
    "${HDFS_LOGS_OUTPUT}" 2>&1

if [ $? -ne 0 ]; then
    echo "ERREUR : Job ErrorCount échoué."
    exit 1
fi

echo ""
echo "[RÉSULTATS] Sortie du job ErrorCount :"
echo "─────────────────────────────────────"
hdfs dfs -cat "${HDFS_LOGS_OUTPUT}/part-r-*"
echo ""

# Affichage des compteurs de Shuffle
echo "[SHUFFLE] Compteurs de transfert réseau :"
echo "          (Voir la ligne 'Shuffle Bytes' dans les logs ci-dessus)"
echo "          Pour extraire les compteurs :"
echo "          yarn logs -applicationId <APP_ID> | grep -E 'SHUFFLE|Bytes Written'"


javac -classpath "${HADOOP_CP}" \
      -d "${WORK_DIR}/classes_amis" \
      "$(dirname "$0")/FriendsCommon.java" 2>&1

if [ $? -ne 0 ]; then
    echo "ERREUR : Compilation de FriendsCommon.java échouée."
    exit 1
fi
echo "      ✓ Compilation réussie."

echo "[JAR] Création de FriendsCommon.jar..."
jar -cvf "${JAR_AMIS}" -C "${WORK_DIR}/classes_amis" . > /dev/null 2>&1
echo "      ✓ JAR créé : ${JAR_AMIS}"

# Suppression de l'ancien output HDFS si existant
hdfs dfs -rm -r -f "${HDFS_GRAPH_OUTPUT}" > /dev/null 2>&1

echo ""
echo "[EXEC] Lancement du job FriendsCommon..."
echo "       Input  : ${HDFS_GRAPH_INPUT}"
echo "       Output : ${HDFS_GRAPH_OUTPUT}"
echo ""

hadoop jar "${JAR_AMIS}" FriendsCommon \
    "${HDFS_GRAPH_INPUT}" \
    "${HDFS_GRAPH_OUTPUT}" 2>&1

if [ $? -ne 0 ]; then
    echo "ERREUR : Job FriendsCommon échoué."
    exit 1
fi

echo ""
echo "[RÉSULTATS] Sortie du job FriendsCommon :"
echo "──────────────────────────────────────────"
hdfs dfs -cat "${HDFS_GRAPH_OUTPUT}/part-r-*"

# ─── Observation YARN ─────────────────────────────────────────────────────────
echo ""
echo "[YARN] Interface de monitoring YARN :"
echo "       → http://localhost:8088"
echo "       Les deux jobs devraient apparaître avec statut SUCCEEDED."
echo ""
echo "[YARN] Compteurs Shuffle pour le dernier job :"
yarn application -list -appStates FINISHED 2>/dev/null | head -5
