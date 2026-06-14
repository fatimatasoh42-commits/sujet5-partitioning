import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * TP2 — Exercice 1 : Comptage des codes d'erreur dans des logs serveur.
 *
 * FORMAT D'ENTRÉE : DATE | IP | URL | STATUS | SIZE
 * Exemple : 2024-01-15 | 192.168.1.10 | /index.html | 404 | 1024
 *
 * SORTIE : CODE_STATUT <TAB> NOMBRE_OCCURRENCES
 * Exemple : 404    153
 *
 * Solution 1 — Classes nommées : LogErrorCountMapper, LogErrorCountCombiner,
 *              LogErrorCountReducer, LogErrorCountJob
 */
public class ErrorCount {

    // ─────────────────────────────────────────────────────────────────────────
    // MAPPER : Extrait le code HTTP et émet (code, 1)
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * LogErrorCountMapper
     * Entrée  : (offset_ligne, texte_ligne)
     * Sortie  : (code_HTTP, IntWritable(1))
     *
     * Le Mapper parse chaque ligne de log, extrait le code de statut HTTP
     * (4ème champ, index 3 après séparation par '|') et n'émet que les codes
     * d'erreur (>= 400).
     */
    public static class LogErrorCountMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        // Constante réutilisable pour éviter des allocations répétées
        private static final IntWritable UN = new IntWritable(1);
        private final Text codeStatut = new Text();

        // Séparateur des champs dans le fichier de logs
        private static final String SEPARATEUR = "\\|";
        // Index du champ STATUS dans la ligne (DATE=0, IP=1, URL=2, STATUS=3, SIZE=4)
        private static final int INDEX_STATUS = 3;
        // Seuil à partir duquel un code est considéré comme une erreur
        private static final int SEUIL_ERREUR = 400;

        @Override
        protected void map(LongWritable cle, Text valeur, Context contexte)
                throws IOException, InterruptedException {

            String ligne = valeur.toString().trim();

            // Ignorer les lignes vides ou les commentaires
            if (ligne.isEmpty() || ligne.startsWith("#")) {
                return;
            }

            // Découpage de la ligne selon le séparateur '|'
            String[] champs = ligne.split(SEPARATEUR);

            // Vérification du nombre de champs minimum attendus
            if (champs.length <= INDEX_STATUS) {
                // Ligne malformée : on ignore et on incrémente le compteur de rejets
                contexte.getCounter("Logs", "Lignes malformées").increment(1);
                return;
            }

            // Extraction et nettoyage du code de statut HTTP
            String codeStr = champs[INDEX_STATUS].trim();

            try {
                int codeHttp = Integer.parseInt(codeStr);

                // On ne garde que les erreurs (codes >= 400)
                if (codeHttp >= SEUIL_ERREUR) {
                    codeStatut.set(codeStr);
                    contexte.write(codeStatut, UN);
                    contexte.getCounter("Logs", "Erreurs émises").increment(1);
                } else {
                    contexte.getCounter("Logs", "Requêtes OK ignorées").increment(1);
                }

            } catch (NumberFormatException e) {
                // Code HTTP non numérique : ligne invalide
                contexte.getCounter("Logs", "Codes invalides").increment(1);
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // COMBINER : Pré-agrégation locale avant le Shuffle
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * LogErrorCountCombiner
     *
     * Le Combiner s'exécute localement sur chaque Mapper AVANT l'envoi des données
     * au Reducer via le réseau (étape Shuffle). Il additionne les compteurs partiels
     * du même nœud, réduisant ainsi significativement le volume de données transférées.
     *
     * Entrée  : (code_HTTP, [1, 1, 1, ...])   ← liste de 1 locaux
     * Sortie  : (code_HTTP, somme_locale)
     *
     * IMPORTANT : Le Combiner peut être la même classe que le Reducer pour un simple
     * comptage, car la somme est associative et commutative.
     */
    public static class LogErrorCountCombiner
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable sommeLocale = new IntWritable();

        @Override
        protected void reduce(Text code, Iterable<IntWritable> valeurs, Context contexte)
                throws IOException, InterruptedException {

            int totalLocal = 0;
            for (IntWritable val : valeurs) {
                totalLocal += val.get();
            }
            sommeLocale.set(totalLocal);
            contexte.write(code, sommeLocale);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // REDUCER : Agrégation finale des compteurs
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * LogErrorCountReducer
     *
     * Reçoit toutes les valeurs pour un même code HTTP (après le Shuffle/Sort).
     * Additionne les compteurs partiels pour obtenir le total global.
     *
     * Entrée  : (code_HTTP, [somme_locale_1, somme_locale_2, ...])
     * Sortie  : (code_HTTP, total_global)
     *
     * Rôle du Reducer dans ce cas :
     * Pour un simple filtrage (extraction des lignes 404), le Reducer peut être
     * omis (Identity Reducer). Mais pour COMPTER les occurrences par code,
     * il est INDISPENSABLE pour l'agrégation finale.
     */
    public static class LogErrorCountReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable totalGlobal = new IntWritable();

        @Override
        protected void reduce(Text code, Iterable<IntWritable> valeurs, Context contexte)
                throws IOException, InterruptedException {

            int compteurTotal = 0;
            for (IntWritable val : valeurs) {
                compteurTotal += val.get();
            }

            totalGlobal.set(compteurTotal);
            contexte.write(code, totalGlobal);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // MAIN : Configuration et lancement du job
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Point d'entrée du job MapReduce.
     *
     * Usage : hadoop jar ErrorCount.jar ErrorCount <chemin_input> <chemin_output>
     *
     * @param args args[0] = répertoire HDFS d'entrée, args[1] = répertoire HDFS de sortie
     */
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage : hadoop jar ErrorCount.jar ErrorCount <input> <output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        conf.set("mapreduce.job.reduces", "1"); // Un seul Reducer pour consolider les compteurs

        Job job = Job.getInstance(conf, "Comptage des erreurs HTTP dans les logs");
        job.setJarByClass(ErrorCount.class);

        // Définition des classes Mapper, Combiner et Reducer
        job.setMapperClass(LogErrorCountMapper.class);
        job.setCombinerClass(LogErrorCountCombiner.class);
        job.setReducerClass(LogErrorCountReducer.class);

        // Types de sortie du Mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // Types de sortie du Reducer (sortie finale)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Formats d'entrée et de sortie
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Chemins HDFS
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Lancement du job et attente de complétion
        boolean succes = job.waitForCompletion(true);

        if (succes) {
            System.out.println("Job terminé avec succès.");
            System.out.println("Vérifiez les compteurs ci-dessus pour les statistiques de Shuffle.");
        }

        System.exit(succes ? 0 : 1);
    }
}