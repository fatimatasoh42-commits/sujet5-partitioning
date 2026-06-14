#!/bin/bash

# TP1 - HDFS : Script complet tp-hadoop.sh


# Exercice 1 : Navigation et Gestion des dossiers

echo ""
echo "[Exercice 1] Création d'arborescence..."

hdfs dfs -mkdir -p $CHEMIN_BASE/data/input
hdfs dfs -mkdir -p $CHEMIN_BASE/data/out
echo "✓ Dossiers créés : $CHEMIN_BASE/data/input et $CHEMIN_BASE/data/out"

echo "Contenu de la racine HDFS :"
hdfs dfs -ls /

echo "Contenu récursif (ls -R) :"
hdfs dfs -ls -R /

echo "Suppression du dossier out..."
hdfs dfs -rm -r $CHEMIN_BASE/data/out
echo "✓ Dossier supprimé. Note : Le dossier .Trash stocke les fichiers supprimés temporairement avant suppression définitive."

# Exercice 2 : Transfert de données (Local ↔ HDFS)

echo ""
echo "[Exercice 2] Transfert de données..."

# Création fichier local de 1 Mo
dd if=/dev/zero of=fichier_1mo.txt bs=1M count=1 2>/dev/null
echo "✓ Fichier local 'fichier_1mo.txt' créé (1 Mo)"

# Upload avec deux méthodes différentes
hdfs dfs -put fichier_1mo.txt $CHEMIN_BASE/data/input/
echo "✓ Upload avec 'put' effectué"

hdfs dfs -copyFromLocal fichier_1mo.txt $CHEMIN_BASE/data/input/fichier_1mo_copy.txt
echo "✓ Upload avec 'copyFromLocal' effectué"

# Download avec changement de nom
hdfs dfs -get $CHEMIN_BASE/data/input/fichier_1mo.txt fichier_renomme.txt
echo "✓ Download effectué sous le nom 'fichier_renomme.txt'"

# Lecture des 10 premières lignes sans télécharger
echo "10 premières lignes du fichier (via cat) :"
hdfs dfs -cat $CHEMIN_BASE/data/input/fichier_1mo.txt | head -10

# Exercice 3 : Gestion des droits et Quotas

echo ""
echo "[Exercice 3] Gestion des droits et quotas..."

hdfs dfs -chmod 444 $CHEMIN_BASE/data/input/fichier_1mo.txt
echo "✓ Fichier passé en lecture seule (chmod 444)"

# Changement propriétaire (à adapter si autre utilisateur existe)
# hdfs dfs -chown autre_utilisateur $CHEMIN_BASE/data/
# echo "✓ Propriétaire changé"

hdfs dfsadmin -setSpaceQuota 50M $CHEMIN_BASE
echo "✓ Quota d'espace de 50 Mo défini sur $CHEMIN_BASE"

echo "Test d'upload d'un fichier de 60 Mo (doit échouer) :"
dd if=/dev/zero of=fichier_60mo.txt bs=1M count=60 2>/dev/null
hdfs dfs -put fichier_60mo.txt $CHEMIN_BASE/data/input/ 2>&1 || echo "⚠️ Échec attendu : quota dépassé"
rm -f fichier_60mo.txt

# Exercice 4 : Fusion et Archivage
echo ""
echo "[Exercice 4] Fusion et archivage..."

# Création de plusieurs petits fichiers pour test
echo "Contenu du fichier A" > local_a.txt
echo "Contenu du fichier B" > local_b.txt
echo "Contenu du fichier C" > local_c.txt

hdfs dfs -put local_a.txt $CHEMIN_BASE/data/input/
hdfs dfs -put local_b.txt $CHEMIN_BASE/data/input/
hdfs dfs -put local_c.txt $CHEMIN_BASE/data/input/

hdfs dfs -getmerge $CHEMIN_BASE/data/input/ fusion_local.txt
echo "✓ Fichiers fusionnés dans 'fusion_local.txt'"

echo "Explication : 'cp' copie directement sur HDFS (plus rapide), 'put' transfère depuis la machine locale."

echo "Statistiques disque du dossier :"
hdfs dfs -du -s -h $CHEMIN_BASE

rm -f local_a.txt local_b.txt local_c.txt fusion_local.txt

# Exercice 5 : Analyse de la distribution des Blocs
echo ""
echo "[Exercice 5] Analyse des blocs..."

hdfs fsck $CHEMIN_BASE/data/input/fichier_1mo.txt -files -blocks -locations


# Exercice 6 : Facteur de Réplication Dynamique

echo ""
echo "[Exercice 6] Modification facteur de réplication..."

hdfs dfs -setrep -w 2 $CHEMIN_BASE/data/input/fichier_1mo.txt
echo "✓ Facteur de réplication passé à 2"

echo "Explication : HDFS ne supprime pas instantanément les répliques en trop pour éviter des transferts réseau inutiles et garantir la disponibilité."

# Exercice 7 : Tolérance aux pannes (simulation)

echo ""
echo "[Exercice 7] Tolérance aux pannes..."

echo " Pour simuler une panne, arrêtez un DataNode :"
echo "   docker stop <nom_conteneur_datanode>  ou  sudo systemctl stop hadoop-datanode"
echo ""
echo "État du cluster :"
hdfs dfsadmin -report | grep -E "(Name:|Hostname:|DFS Used:|Dead)"
echo ""
echo "Le NameNode déclare un nœud 'Dead' après environ 10 minutes par défaut (paramètre dfs.namenode.heartbeat.recheck-interval)."

# Exercice 8 : Mode Safe Mode

echo ""
echo "[Exercice 8] Safe Mode..."

hdfs dfsadmin -safemode enter
echo " Safe mode activé"

echo "Tentative de suppression en safemode :"
hdfs dfs -rm $CHEMIN_BASE/data/input/fichier_1mo.txt 2>&1 || echo "⚠️ Suppression impossible en safemode"

hdfs dfsadmin -safemode leave
echo "✓ Safe mode désactivé"

echo "Vérification de l'intégrité :"
hdfs fsck /
