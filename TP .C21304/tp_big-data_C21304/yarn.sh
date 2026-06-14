#!/bin/bash



# ─── Exercice 1 : Exploration de l'interface ResourceManager ─────────────────
echo ""
echo "─── Exercice 1 : Interface ResourceManager (Port 8088) ───"
echo ""
echo "[1.1] Interface Web YARN disponible sur :"
echo "      http://localhost:8088"
echo ""
echo "[1.2] Informations sur les nœuds actifs et la mémoire disponible :"
echo "      Via CLI :"
yarn node -list -all 2>/dev/null
echo ""
echo "      Via API REST :"
curl -s http://localhost:8088/ws/v1/cluster/metrics 2>/dev/null | \
  python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    m = data.get('clusterMetrics', {})
    print(f'  Nœuds actifs       : {m.get(\"activeNodes\", \"N/A\")}')
    print(f'  Mémoire totale     : {m.get(\"totalMB\", \"N/A\")} Mo')
    print(f'  Mémoire allouée    : {m.get(\"allocatedMB\", \"N/A\")} Mo')
    print(f'  Mémoire disponible : {m.get(\"availableMB\", \"N/A\")} Mo')
    print(f'  vCores totaux      : {m.get(\"totalVirtualCores\", \"N/A\")}')
    print(f'  Applications actives : {m.get(\"activeApps\", \"N/A\")}')
except:
    print('  (Cluster non démarré ou API non disponible)')
" 2>/dev/null || echo "  (Démarrez le cluster avec : docker-compose -f yarn.yml up -d)"

echo ""
echo "[1.3] Lancement d'un job d'exemple pour observer l'ApplicationMaster :"
echo "      Commande à exécuter :"
echo "      hadoop jar \${HADOOP_HOME}/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar pi 10 100"
echo ""
echo "      Puis accédez à http://localhost:8088 pour voir l'ApplicationMaster."
echo "      Pendant l'exécution, observez :"
echo "      → L'ApplicationMaster apparaît dans la liste des applications"
echo "      → Les containers sont alloués aux NodeManagers"
echo "      → La progression des Maps et Reduces en temps réel"

# ─── Exercice 2 : Commandes CLI YARN ─────────────────────────────────────────
echo ""
echo "─── Exercice 2 : Commandes CLI YARN ───"
echo ""

# 2.1 Liste des applications en cours
echo "[2.1] Applications YARN en cours d'exécution :"
yarn application -list 2>/dev/null || echo "      (Aucune application en cours)"

echo ""
echo "[2.1] Applications terminées (état FINISHED) :"
yarn application -list -appStates FINISHED 2>/dev/null | head -10

echo ""
echo "[2.1] Applications en échec :"
yarn application -list -appStates FAILED 2>/dev/null | head -5

# 2.2 Arrêt d'une application
echo ""
echo "[2.2]  Forcer l'arrêt d'une application (kill) :"
echo "      Syntaxe : yarn application -kill <APPLICATION_ID>"
echo "      Exemple : yarn application -kill application_1234567890123_0001"
echo ""
echo "      Pour trouver l'ID d'une application :"
echo "      yarn application -list"
echo ""
echo "        Cette commande est irréversible ! Le job devra être relancé."
echo ""
# Simulation (à décommenter avec un vrai APP_ID) :
# APP_ID=$(yarn application -list 2>/dev/null | grep "RUNNING" | awk '{print $1}' | head -1)
# if [ -n "${APP_ID}" ]; then
#     echo "      ⚠️  Arrêt de : ${APP_ID}"
#     yarn application -kill "${APP_ID}"
# fi

# 2.3 Logs d'une application
echo "[2.3] Consultation des logs d'un job terminé :"
echo "      Syntaxe : yarn logs -applicationId <APPLICATION_ID>"
echo ""

# Récupération du dernier job FINISHED pour afficher ses logs
LAST_APP=$(yarn application -list -appStates FINISHED 2>/dev/null | \
           grep "^application_" | awk '{print $1}' | tail -1)

if [ -n "${LAST_APP}" ]; then
    echo "      Logs du dernier job terminé (${LAST_APP}) :"
    yarn logs -applicationId "${LAST_APP}" 2>/dev/null | tail -30
else
    echo "      (Aucun job terminé disponible. Lancez d'abord un job MapReduce.)"
    echo "      Exemple de commande :"
    echo "      yarn logs -applicationId application_1234567890123_0001"
fi

echo ""
echo "[2.3] IMPORTANCE de l'agrégation de logs (Log Aggregation) :"
echo "      Sans agrégation : les logs sont stockés localement sur chaque NodeManager."
echo "      → Problème : si le NodeManager tombe, les logs sont perdus."
echo "      → Problème : il faut se connecter à chaque nœud séparément pour déboguer."
echo "      Avec agrégation (yarn.log-aggregation-enable=true) :"
echo "      → Les logs de tous les containers sont copiés vers HDFS à la fin du job."
echo "      → Accessibles via 'yarn logs -applicationId' depuis n'importe quel nœud."
echo "      → Conservation configurable (yarn.log-aggregation.retain-seconds)."
echo "      → Indispensable en production pour le débogage post-mortem."

# ─── Exercice 3 : Configuration des Containers ───────────────────────────────
echo ""
echo "─── Exercice 3 : Configuration des Containers ───"
echo ""
echo "[3.1] Propriétés de mémoire dans yarn-site.xml :"
echo ""
echo "      ┌─────────────────────────────────────────────────────────────────┐"
echo "      │ Propriété                              │ Valeur (Solution 1)    │"
echo "      ├─────────────────────────────────────────────────────────────────┤"
echo "      │ yarn.nodemanager.resource.memory-mb    │ 4096 Mo (total nœud)   │"
echo "      │ yarn.scheduler.minimum-allocation-mb   │ 256 Mo (granularité)   │"
echo "      │ yarn.scheduler.maximum-allocation-mb   │ 2048 Mo (max/container)│"
echo "      │ yarn.nodemanager.resource.cpu-vcores   │ 4 vCores               │"
echo "      │ yarn.scheduler.minimum-allocation-vcores│ 1 vCore               │"
echo "      │ yarn.scheduler.maximum-allocation-vcores│ 4 vCores              │"
echo "      └─────────────────────────────────────────────────────────────────┘"
echo ""
echo "[3.2] Comportement si un job demande 4 Go alors que le max est 2 Go :"
echo "      → YARN tronque la demande au maximum configuré (2048 Mo)."
echo "      → Le container est alloué avec 2048 Mo (pas 4096 Mo)."
echo "      → Un avertissement est écrit dans les logs du ResourceManager :"
echo "        'Requested resource 4096 MB > maximum 2048 MB, reducing to maximum.'"
echo "      → Le job peut échouer avec OutOfMemoryError si 2 Go sont insuffisants."
echo "      → Solution : augmenter yarn.scheduler.maximum-allocation-mb"
echo "                   OU optimiser le job pour utiliser moins de mémoire."

# ─── Exercice 4 : Schedulers ─────────────────────────────────────────────────
echo ""
echo "─── Exercice 4 : Schedulers YARN ───"
echo ""

echo "[4.1] Capacity Scheduler — Fonctionnement :"
echo ""
echo "      Le Capacity Scheduler divise les ressources du cluster en QUEUES."
echo "      Chaque queue a une capacité garantie (en % des ressources totales)."
echo ""
echo "      Exemple de configuration multi-département :"
echo ""
echo "      Cluster total : 100% des ressources"
echo "      ├── Queue 'finance'    : 40% garantis → jobs financiers prioritaires"
echo "      ├── Queue 'datascience': 35% garantis → jobs ML/analytics"
echo "      └── Queue 'default'    : 25% garantis → autres jobs"
echo ""
echo "      Comportement :"
echo "      1. Chaque département a sa capacité GARANTIE minimum."
echo "      2. Si 'finance' n'utilise que 20%, les 20% excédentaires sont"
echo "         temporairement prêtés aux autres queues (elasticity)."
echo "      3. Quand 'finance' a de nouveaux jobs, les ressources prêtées"
echo "         sont récupérées progressivement."
echo "      4. Les queues peuvent avoir des sous-queues (hiérarchie)."
echo "      5. Des ACLs (Access Control Lists) contrôlent qui peut soumettre"
echo "         des jobs dans quelle queue."
echo ""
echo "      Avantage vs FIFO : isolation des ressources entre équipes,"
echo "      pas de 'job monstre' qui monopolise tout le cluster."

echo ""
echo "[4.2] Préemption dans YARN — Définition et Fonctionnement :"
echo ""
echo "      La PRÉEMPTION est le mécanisme par lequel YARN peut forcer l'arrêt"
echo "      de containers appartenant à une application basse-priorité pour"
echo "      libérer des ressources pour une application haute-priorité."
echo ""
echo "      Scénario typique :"
echo "      → Queue 'finance' (40% garanti) utilise 0% car aucun job actif."
echo "      → Queue 'datascience' emprunte ces 40% (utilise 75% total)."
echo "      → Un nouveau job urgent arrive dans 'finance'."
echo "      → Le Scheduler décide de récupérer les 40% de 'finance'."
echo "      → Avec préemption : YARN envoie un signal d'arrêt aux containers"
echo "        de 'datascience' occupant les ressources de 'finance'."
echo "      → Les containers préemptés sont tués (perte de leur travail en cours)."
echo "      → Les ressources sont libérées pour le job 'finance'."
echo ""
echo "      Configuration de la préemption (yarn-site.xml) :"
echo "      - yarn.resourcemanager.scheduler.monitor.enable=true"
echo "      - yarn.resourcemanager.monitor.capacity.preemption.enabled=true"
echo "      - yarn.resourcemanager.monitor.capacity.preemption.max_wait_before_kill=15000"
echo "        (délai avant de tuer un container en ms, défaut 15 secondes)"
echo ""
echo "       La préemption peut causer des pertes de travail partiels."
echo "      Les frameworks comme Spark peuvent gérer la préemption en"
echo "      sauvegardant leur état (checkpointing) avant d'être tués."

echo ""
echo "[STATUT] État actuel du cluster YARN :"
yarn node -list 2>/dev/null || echo "      (Démarrez le cluster avec : docker-compose -f yarn.yml up -d)"

echo ""
echo "[QUEUES] État des queues :"
yarn queue -status default 2>/dev/null || echo "      (Cluster non disponible)"
