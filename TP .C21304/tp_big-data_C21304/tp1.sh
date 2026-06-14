#!/bin/bash


# PARTIE I — EXERCICE 1 : Navigation et Gestion des dossiers
echo ""
echo "─── Exercice 1 : Navigation et Gestion des dossiers ───"

# 1.1 Création de l'arborescence HDFS
echo "[1.1] Création des dossiers HDFS..."
hdfs dfs -mkdir -p "${DOSSIER_ENTREE}"
hdfs dfs -mkdir -p "${DOSSIER_SORTIE}"
echo "      ✓ Dossiers créés : ${DOSSIER_ENTREE} et ${DOSSIER_SORTIE}"

# 1.2 Différence entre ls et ls -R
echo ""
echo "[1.2] Listage de la racine HDFS avec 'ls' (niveau 1 uniquement) :"
hdfs dfs -ls /

echo ""
echo "[1.2] Listage récursif avec 'ls -R' (tous les sous-dossiers) :"
hdfs dfs -ls -R "${RACINE_HDFS}"
# EXPLICATION :
# 'hdfs dfs -ls'   → liste uniquement les fichiers/dossiers du répertoire courant (1 niveau)
# 'hdfs dfs -ls -R' → liste récursivement tous les fichiers dans l'arborescence complète

# 1.3 Suppression du dossier output et explication du .Trash
echo ""
echo "[1.3]   Suppression du dossier output..."
hdfs dfs -rm -r "${DOSSIER_SORTIE}"
echo ""
echo "      INFO — Le dossier .Trash :"
echo "      Lorsqu'un fichier est supprimé avec 'hdfs dfs -rm', il n'est pas"
echo "      immédiatement effacé mais déplacé dans /user/<nom>/.Trash/Current/"
echo "      Il y reste pendant une durée configurable (fs.trash.interval)."
echo "      Pour supprimer définitivement SANS passer par la corbeille : -rm -skipTrash"


echo ""
echo "─── Exercice 2 : Transfert de données ───"

# 2.1 Création d'un fichier local de 1 Mo
echo "[2.1] Création d'un fichier local de 1 Mo..."
dd if=/dev/urandom of="${FICHIER_LOCAL_TEMP}" bs=1024 count=1024 2>/dev/null
echo "      ✓ Fichier créé : ${FICHIER_LOCAL_TEMP} ($(du -sh ${FICHIER_LOCAL_TEMP} | cut -f1))"

# Chargement sur HDFS — méthode 1 : put
echo "      Chargement via 'put'..."
hdfs dfs -put "${FICHIER_LOCAL_TEMP}" "${DOSSIER_ENTREE}/fichier_via_put.txt"
echo "      ✓ Méthode 1 (put) terminée."

# Chargement sur HDFS — méthode 2 : copyFromLocal
echo "      Chargement via 'copyFromLocal'..."
hdfs dfs -copyFromLocal "${FICHIER_LOCAL_TEMP}" "${DOSSIER_ENTREE}/fichier_via_copyfromlocal.txt"
echo "      ✓ Méthode 2 (copyFromLocal) terminée."
# NOTE : 'put' et 'copyFromLocal' sont fonctionnellement identiques.

# 2.2 Téléchargement d'un fichier HDFS vers local avec renommage
echo ""
echo "[2.2] Téléchargement de HDFS vers local (avec renommage)..."
hdfs dfs -get "${DOSSIER_ENTREE}/fichier_via_put.txt" "/tmp/fichier_telecharge_alpha.txt"
echo "      ✓ Fichier téléchargé sous : /tmp/fichier_telecharge_alpha.txt"

# 2.3 Lecture des 10 premières lignes directement depuis HDFS
echo ""
echo "[2.3] Lecture des 10 premières lignes sans téléchargement (cat | head) :"
hdfs dfs -cat "${DOSSIER_ENTREE}/fichier_via_put.txt" | head -n 10

# PARTIE I — EXERCICE 3 : Gestion des droits et Quotas
echo ""
echo "─── Exercice 3 : Droits et Quotas ───"

# 3.1 Fichier en lecture seule pour tout le monde
echo "[3.1] Passage en lecture seule (chmod 444)..."
hdfs dfs -chmod 444 "${DOSSIER_ENTREE}/fichier_via_put.txt"
echo "      ✓ Permissions modifiées → 444 (r--r--r--)"

# 3.2 Changement de propriétaire
echo ""
echo "[3.2]  Changement de propriétaire (chown)..."
hdfs dfs -chown hdfs:supergroup "${RACINE_HDFS}/data"
echo "      ✓ Propriétaire modifié → hdfs:supergroup"

# 3.3 Quota d'espace et test de dépassement
echo ""
echo "[3.3] Définition d'un quota de ${TAILLE_QUOTA_MO} Mo sur le dossier personnel..."
hdfs dfsadmin -setSpaceQuota ${TAILLE_QUOTA_MO}m "${RACINE_HDFS}"
echo "      ✓ Quota appliqué."

echo "      Test : création d'un fichier de 60 Mo (dépassement du quota)..."
dd if=/dev/zero of="${FICHIER_LOCAL_GRAND}" bs=1M count=60 2>/dev/null
echo "       Tentative d'upload du fichier de 60 Mo :"
hdfs dfs -put "${FICHIER_LOCAL_GRAND}" "${RACINE_HDFS}/gros_fichier.bin" 2>&1 || \
  echo "      ✗ Erreur attendue : NSQuotaExceededException — quota dépassé."
echo "      INFO : HDFS refuse l'écriture et retourne une exception de quota."

# PARTIE I — EXERCICE 4 : Fusion et Archivage


# Préparation : création de petits fichiers texte
echo "[4.0] Préparation de petits fichiers texte sur HDFS..."
for i in 1 2 3; do
  echo "Contenu du fichier fragment ${i}" > "/tmp/fragment_alpha_${i}.txt"
  hdfs dfs -put "/tmp/fragment_alpha_${i}.txt" "${DOSSIER_ENTREE}/fragment_${i}.txt"
done
echo "      ✓ Fragments créés."

# 4.1 getmerge
echo ""
echo "[4.1] Fusion des fragments avec 'getmerge'..."
hdfs dfs -getmerge "${DOSSIER_ENTREE}/fragment_*.txt" "/tmp/fusion_alpha.txt"
echo "      ✓ Fichier fusionné : /tmp/fusion_alpha.txt"
echo "      Contenu :"
cat "/tmp/fusion_alpha.txt"

# 4.2 cp vs put
echo ""
echo "[4.2] Comparaison cp vs put :"
echo "      'hdfs dfs -put'  : copie depuis le système de fichiers LOCAL vers HDFS."
echo "                         Le fichier transite par le réseau depuis la machine cliente."
echo "      'hdfs dfs -cp'   : copie un fichier déjà présent sur HDFS vers un autre"
echo "                         emplacement HDFS, sans passer par la machine cliente."
echo "                         Avantage : beaucoup plus rapide car les données restent"
echo "                         dans le cluster (copie serveur-to-serveur via DataNodes)."
hdfs dfs -cp "${DOSSIER_ENTREE}/fragment_1.txt" "${DOSSIER_ENTREE}/fragment_1_copie.txt"
echo "      ✓ Copie HDFS→HDFS effectuée avec 'cp'."

# 4.3 Statistiques d'utilisation du disque
echo ""
echo "[4.3] Statistiques disque du dossier (du -s -h) :"
hdfs dfs -du -s -h "${RACINE_HDFS}"



echo "[5.1] Analyse FSCK sur un fichier volumineux :"
echo "      (Utilisation de fragment_1.txt pour la démonstration)"
hdfs fsck "${DOSSIER_ENTREE}/fichier_via_put.txt" -files -blocks -locations

echo ""
echo "[5.3] EXPLICATION — Pourquoi un fichier de 10 octets occupe une entrée NameNode ?"
echo "      HDFS fonctionne par BLOCS (128 Mo par défaut). Un fichier de 10 octets"
echo "      occupe quand même 1 bloc logique complet dans les métadonnées du NameNode."
echo "      L'espace PHYSIQUE utilisé sur le DataNode est effectivement de 10 octets,"
echo "      mais le NameNode enregistre toujours un inode + au moins un BlockID."
echo "      C'est pourquoi HDFS est inadapté à des millions de petits fichiers"
echo "      (surcharge mémoire du NameNode = problème des 'small files')."


echo "[6.1] Modification du facteur de réplication de 3 → 2 :"
hdfs dfs -setrep -w 2 "${DOSSIER_ENTREE}/fichier_via_put.txt"
echo "      ✓ Réplication modifiée à 2 (le flag -w attend la fin de la réplication)."

echo ""
echo "[6.2] Vérification via l'interface Web du NameNode :"
echo "      → Accédez à http://<namenode-ip>:9870 → Utilities → Browse the file system"
echo "      → Cliquez sur le fichier pour voir le facteur de réplication actuel."

echo ""
echo "[6.3] EXPLICATION — Pourquoi HDFS ne supprime pas instantanément les répliques ?"
echo "      Le NameNode envoie des instructions de suppression aux DataNodes lors"
echo "      des heartbeats (toutes les 3 secondes). La suppression est différée pour :"
echo "      1. Éviter la surcharge réseau (pas de burst de suppressions simultanées)"
echo "      2. Garantir d'abord la création des nouvelles répliques manquantes"
echo "      3. Maintenir le facteur de réplication >= minimum pendant la transition"
echo "      Les répliques excédentaires sont marquées 'invalidées' et supprimées"
echo "      progressivement lors des prochains cycles de heartbeat."


echo "[7.1] Identification d'un DataNode hébergeant une réplique :"
hdfs fsck "${DOSSIER_ENTREE}/fichier_via_put.txt" -files -blocks -locations 2>/dev/null | \
  grep -A2 "Block replica"

echo ""
echo "[7.2]   Arrêt d'un DataNode (DANGER — commande simulée) :"
echo "      Dans un vrai cluster : \$HADOOP_HOME/sbin/hadoop-daemon.sh stop datanode"
echo "      Avec Docker          : docker stop datanode1"
echo "       NE PAS exécuter en production sans planification de maintenance !"
# COMMANDE SIMULÉE — à décommenter uniquement en environnement de test :
# docker stop datanode1

echo ""
echo "[7.3] Rapport du cluster (dfsadmin -report) :"
hdfs dfsadmin -report
echo ""
echo "      INFO — Délai de détection 'Dead' :"
echo "      Par défaut, le NameNode attend 10 minutes (dfs.namenode.heartbeat.recheck-interval"
echo "      + dfs.heartbeat.interval) avant de déclarer un DataNode 'Dead'."
echo "      Paramètres clés :"
echo "      - dfs.heartbeat.interval = 3 secondes (heartbeat entre DataNode et NameNode)"
echo "      - dfs.namenode.heartbeat.recheck-interval = 5 minutes"
echo "      - Un nœud est déclaré Dead après ~10 min sans heartbeat."

echo ""
echo "[7.4] Réaction du système pour garantir la réplication :"
echo "      Le NameNode détecte le manque de répliques lors du prochain rapport de blocs."
echo "      Il ordonne à un DataNode vivant de copier les blocs affectés vers un autre"
echo "      nœud disponible, restaurant ainsi le facteur de réplication configuré."


# EXERCICE 8 : Safe Mode

echo ""
echo "─── Exercice 8 : Safe Mode et Maintenance ───"

echo "[8.1]  Activation forcée du Safe Mode :"
hdfs dfsadmin -safemode enter
echo "      ✓ Safe Mode activé."

echo ""
echo "      Test de suppression en Safe Mode (doit échouer) :"
hdfs dfs -rm "${DOSSIER_ENTREE}/fragment_1.txt" 2>&1 || \
  echo "      ✗ Erreur attendue : Cannot delete in safe mode — HDFS est en lecture seule."

echo ""
echo "[8.2] Conditions réelles d'activation automatique du Safe Mode :"
echo "      Le NameNode entre automatiquement en Safe Mode lors :"
echo "      1. Du DÉMARRAGE : le NameNode charge les métadonnées (fsimage + edit logs)"
echo "         et attend que suffisamment de DataNodes rapportent leurs blocs."
echo "      2. D'un TAUX DE RÉPLICATION insuffisant : si moins de dfs.safemode.threshold.pct"
echo "         (défaut 99.9%) des blocs ont le nombre minimum de répliques requises."
echo "      3. D'un ESPACE DISQUE critique sur les DataNodes."
echo "      En Safe Mode, HDFS est en lecture seule : aucune écriture ni suppression."

echo ""
echo "[8.3]   Sortie du Safe Mode et vérification d'intégrité :"
hdfs dfsadmin -safemode leave
echo "      ✓ Safe Mode désactivé."

echo ""
echo "      Vérification de l'intégrité du système de fichiers :"
hdfs fsck / -summary
echo ""
echo "      Vérification manuelle de l'état du Safe Mode :"
hdfs dfsadmin -safemode get

