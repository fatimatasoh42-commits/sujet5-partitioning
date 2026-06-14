#!/bin/bash


# Vérification
if ! command -v yarn &> /dev/null; then
    echo "Erreur: YARN n'est pas disponible"
    exit 1
fi

# 1. Liste des applications
echo ""
echo "[1] Liste des applications en cours :"
yarn application -list

# 2. Tuer une application (à remplacer par un vrai ID)
echo ""
echo "[2] Pour tuer une application :"
echo "    yarn application -kill application_XXXXX_XXXXX"
echo " Cette commande arrête immédiatement le job"

# 3. Logs d'une application
echo ""
echo "[3] Logs d'une application terminée :"
echo "    yarn logs -applicationId application_XXXXX_XXXXX"
echo ""
echo "L'agrégation de logs (Log Aggregation) est cruciale car :"
echo "- Elle centralise les logs sur HDFS"
echo "- Permet de consulter les logs même après la mort du conteneur"
echo "- Évite de se connecter à chaque nœud individuellement"

# 4. Liste des nœuds
echo ""
echo "[4] Liste des nœuds du cluster :"
yarn node -list

# 5. Statistiques des files d'attente
echo ""
echo "[5] Statut des files d'attente (queues) :"
yarn queue -status default

# 6. Explications théoriques
echo ""

echo "Explications théoriques"
echo ""
echo "--- Capacity Scheduler ---"
echo "Permet de partitionner les ressources du cluster en plusieurs files d'attente."
echo "Chaque département (ex: Finance) peut avoir un minimum garanti."
echo "Exemple: queue finance avec 30% des ressources, queue marketing avec 70%"
echo ""
echo "--- Préemption ---"
echo "Si une queue n'utilise pas ses ressources, elles sont données à d'autres."
echo "Mais si la queue prioritaire en a besoin, YARN reprend (préempte) les ressources."
echo "Cela peut tuer des conteneurs en cours d'exécution."
echo " À activer avec précaution !"
echo ""
echo "--- FIFO Scheduler vs Capacity Scheduler ---"
echo "FIFO: Premier arrivé, premier servi. Un gros job bloque les petits."
echo "Capacity: Plusieurs jobs peuvent tourner en parallèle dans différentes queues."