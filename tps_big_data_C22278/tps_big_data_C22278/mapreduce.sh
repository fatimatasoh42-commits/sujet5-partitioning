#!/bin/bash


# Vérification des paramètres Hadoop
if ! command -v hadoop &> /dev/null; then
    echo "Erreur: Hadoop n'est pas installé ou pas dans le PATH"
    exit 1
fi

# Nettoyage des anciens fichiers
rm -f HadoopJobs*.class
rm -rf output_errors output_friends

# Compilation
echo "Compilation des classes Java..."
javac -cp $(hadoop classpath) HadoopJobs.java

if [ $? -ne 0 ]; then
    echo "Erreur lors de la compilation"
    exit 1
fi
echo "✓ Compilation réussie"

# Création du JAR
echo "Création du fichier JAR..."
jar cf HadoopJobs.jar *.class
echo "✓ JAR créé"

# Exécution du job ErrorCount
echo ""
echo "--- Job 1 : ErrorCount (comptage des erreurs) ---"
echo "Entrée: /user/votre_nom/data/logs"
echo "Sortie: /user/votre_nom/data/output_errors"

# Création d'un fichier log exemple si nécessaire
echo "DATE|IP|URL|STATUS|SIZE" > logs_exemple.txt
echo "2024-01-01|192.168.1.1|/home|200|1024" >> logs_exemple.txt
echo "2024-01-01|192.168.1.2|/login|404|512" >> logs_exemple.txt
echo "2024-01-01|192.168.1.3|/admin|500|256" >> logs_exemple.txt
echo "2024-01-01|192.168.1.1|/home|404|2048" >> logs_exemple.txt
echo "2024-01-01|192.168.1.4|/api|403|128" >> logs_exemple.txt

hdfs dfs -mkdir -p /user/votre_nom/data/logs
hdfs dfs -put -f logs_exemple.txt /user/votre_nom/data/logs/
rm -f logs_exemple.txt

hdfs dfs -rm -r -f /user/votre_nom/data/output_errors 2>/dev/null

hadoop jar HadoopJobs.jar ErrorCount /user/votre_nom/data/logs /user/votre_nom/data/output_errors

echo ""
echo "Résultats du job ErrorCount :"
hdfs dfs -cat /user/votre_nom/data/output_errors/part-r-00000

# Exécution du job FriendsCommon
echo ""
echo "--- Job 2 : FriendsCommon (amis communs) ---"

# Création d'un fichier exemple d'amis
echo "A -> B,C,D" > amis_exemple.txt
echo "B -> A,C" >> amis_exemple.txt
echo "C -> A,B,E" >> amis_exemple.txt
echo "D -> A" >> amis_exemple.txt
echo "E -> C" >> amis_exemple.txt

hdfs dfs -mkdir -p /user/votre_nom/data/friends
hdfs dfs -put -f amis_exemple.txt /user/votre_nom/data/friends/
rm -f amis_exemple.txt

hdfs dfs -rm -r -f /user/votre_nom/data/output_friends 2>/dev/null

hadoop jar HadoopJobs.jar FriendsCommon /user/votre_nom/data/friends /user/votre_nom/data/output_friends

echo ""
echo "Résultats du job FriendsCommon :"
hdfs dfs -cat /user/votre_nom/data/output_friends/part-r-00000

