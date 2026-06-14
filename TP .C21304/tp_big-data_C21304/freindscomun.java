import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

/**
 * TP2 — Exercice 3 : Calcul des amis communs entre chaque paire d'utilisateurs.
 *
 * FORMAT D'ENTRÉE : Utilisateur<TAB>ami1,ami2,ami3,...
 * Exemple : A    B,C,D
 *           B    A,C,E
 *           C    A,B,D
 *
 * SORTIE : (U1,U2)<TAB>[ami_commun1, ami_commun2, ...]
 * Exemple : (A,B)    [C]
 *           (A,C)    [B, D]
 *
 * Solution 1 — Classes : SocialGraphMapper, CommonFriendsReducer, CommonFriendsJob
 *
 * ALGORITHME :
 * Phase MAP : Pour chaque utilisateur U avec sa liste d'amis [F1, F2, F3, ...],
 *             on génère toutes les paires (Fi, Fj) avec i < j,
 *             et on émet : clé=(Fi,Fj), valeur=liste_amis_de_U
 * Phase REDUCE : Pour chaque paire (Fi, Fj), on reçoit les listes d'amis
 *                de tous leurs amis communs → on fait l'intersection.
 *
 * GESTION DES CÉLÉBRITÉS :
 * Un utilisateur avec 10M d'amis génère C(10M, 2) = ~50 milliards de paires,
 * ce qui saturerait le Shuffle. Voir commentaire détaillé dans le Mapper.
 */
public class FriendsCommon {

    // Seuil maximum d'amis avant d'activer la protection anti-célébrité
    private static final int SEUIL_CELEBRITE = 10_000;

    // ─────────────────────────────────────────────────────────────────────────
    // MAPPER : Génération des paires (ami_i, ami_j) → liste_amis_de_U
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * SocialGraphMapper
     *
     * Pour chaque ligne "U → [F1, F2, ..., Fn]", on génère toutes les paires
     * d'amis (Fi, Fj) avec i < j (pour éviter les doublons symétriques),
     * et on émet la liste complète des amis de U comme valeur.
     *
     * Entrée  : (offset, "U<TAB>F1,F2,F3")
     * Sortie  : ("(Fi,Fj)", "F1,F2,F3,...")  pour chaque paire Fi < Fj dans les amis de U
     *
     * PROBLÈME DE LA CÉLÉBRITÉ (scalabilité) :
     * Si un utilisateur "star" a N=10M amis, le Mapper génère C(N,2) = N*(N-1)/2
     * ≈ 50 milliards de paires. Cela provoque :
     * 1. Une explosion mémoire dans le Mapper
     * 2. Une surcharge massive de l'étape Shuffle (données → 50B * taille_clé)
     * 3. Un déséquilibre : quelques Reducers reçoivent des milliards de valeurs
     *    → "hot partition" ou "data skew"
     * Solution : filtrer les utilisateurs avec trop d'amis OU utiliser un
     * algorithme par blocs (partitionner les amis en sous-ensembles).
     */
    public static class SocialGraphMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        private final Text paireCle = new Text();
        private final Text listeAmisValeur = new Text();

        @Override
        protected void map(LongWritable cle, Text valeur, Context contexte)
                throws IOException, InterruptedException {

            String ligne = valeur.toString().trim();
            if (ligne.isEmpty() || ligne.startsWith("#")) {
                return;
            }

            // Séparation : "U<TAB>F1,F2,F3" → utilisateur et liste d'amis
            String[] parties = ligne.split("\t", 2);
            if (parties.length < 2) {
                contexte.getCounter("Réseau social", "Lignes malformées").increment(1);
                return;
            }

            String utilisateur = parties[0].trim();
            String[] amis = parties[1].trim().split(",");

            // Nettoyage des espaces autour des noms d'amis
            for (int i = 0; i < amis.length; i++) {
                amis[i] = amis[i].trim();
            }

            // Gestion des célébrités : on ignore les profils avec trop d'amis
            if (amis.length > SEUIL_CELEBRITE) {
                contexte.getCounter("Réseau social", "Célébrités ignorées").increment(1);
                System.err.println("AVERTISSEMENT : Utilisateur " + utilisateur
                        + " a " + amis.length + " amis (> seuil=" + SEUIL_CELEBRITE
                        + "). Ignoré pour éviter l'explosion du Shuffle.");
                return;
            }

            // Tri des amis pour garantir l'ordre canonique dans les paires
            Arrays.sort(amis);
            String listeAmisStr = String.join(",", amis);
            listeAmisValeur.set(listeAmisStr);

            // Génération de toutes les paires (ami_i, ami_j) avec i < j
            // Pour chaque paire, U est un ami commun potentiel
            for (int i = 0; i < amis.length - 1; i++) {
                for (int j = i + 1; j < amis.length; j++) {
                    // Clé canonique : toujours (min, max) pour éviter les doublons
                    String ami1 = amis[i].compareTo(amis[j]) <= 0 ? amis[i] : amis[j];
                    String ami2 = amis[i].compareTo(amis[j]) <= 0 ? amis[j] : amis[i];

                    paireCle.set("(" + ami1 + "," + ami2 + ")");
                    contexte.write(paireCle, listeAmisValeur);
                }
            }

            contexte.getCounter("Réseau social", "Utilisateurs traités").increment(1);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // REDUCER : Calcul de l'intersection des listes → amis communs
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * CommonFriendsReducer
     *
     * Pour chaque paire (U1, U2), reçoit la liste des amis de tous leurs
     * amis communs connus. Calcule l'intersection pour trouver les vrais
     * amis communs.
     *
     * Entrée  : ("(U1,U2)", ["F1,F2,F3", "F2,F3,F4", ...])
     * Sortie  : ("(U1,U2)", "[F2, F3]")  ← amis communs de U1 et U2
     *
     * Explication de l'intersection :
     * Chaque liste de valeurs représente les amis d'un utilisateur commun.
     * Un ami X est commun à (U1, U2) si X apparaît dans la liste d'amis de U1
     * ET dans la liste d'amis de U2. Dans notre modèle, cela correspond au fait
     * que la liste d'amis de X contient à la fois U1 et U2, ce qui se traduit
     * par l'émission de la paire (U1, U2) avec la liste d'amis de X.
     * Le Reducer doit compter les listes reçues : si (U1, U2) apparaît dans
     * la liste d'amis de X, alors X est un ami commun.
     */
    public static class CommonFriendsReducer
            extends Reducer<Text, Text, Text, Text> {

        private final Text resultat = new Text();

        @Override
        protected void reduce(Text paire, Iterable<Text> listes, Context contexte)
                throws IOException, InterruptedException {

            // Extraction des deux membres de la paire
            // Format de la clé : "(U1,U2)"
            String pairStr = paire.toString();
            String contenu = pairStr.substring(1, pairStr.length() - 1); // enlève les parenthèses
            String[] membres = contenu.split(",", 2);

            if (membres.length < 2) {
                return;
            }

            String u1 = membres[0];
            String u2 = membres[1];

            // Collecte de toutes les listes d'amis reçues
            // Chaque liste représente les amis d'un utilisateur qui connaît U1 ET U2
            List<TreeSet<String>> toutesLesListes = new ArrayList<>();

            for (Text liste : listes) {
                String[] amis = liste.toString().split(",");
                TreeSet<String> ensembleAmis = new TreeSet<>();
                for (String ami : amis) {
                    ensembleAmis.add(ami.trim());
                }
                toutesLesListes.add(ensembleAmis);
            }

            if (toutesLesListes.isEmpty()) {
                return;
            }

            // Intersection de toutes les listes pour trouver les amis communs
            // Un ami commun de (U1, U2) est un utilisateur X tel que X est ami de U1
            // ET X est ami de U2. Dans notre encodage, cela signifie que la liste d'amis
            // de X contient U1 ET U2, donc la paire (U1,U2) est émise avec la liste de X.
            // Ici, nous collectons les listes et faisons l'intersection.
            TreeSet<String> amisCommuns = new TreeSet<>(toutesLesListes.get(0));
            for (int i = 1; i < toutesLesListes.size(); i++) {
                amisCommuns.retainAll(toutesLesListes.get(i));
            }

            // Exclusion de U1 et U2 eux-mêmes de la liste des amis communs
            amisCommuns.remove(u1);
            amisCommuns.remove(u2);

            if (!amisCommuns.isEmpty()) {
                resultat.set("[" + String.join(", ", amisCommuns) + "]");
                contexte.write(paire, resultat);
                contexte.getCounter("Réseau social", "Paires avec amis communs").increment(1);
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // MAIN : Configuration et lancement du job
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Point d'entrée du job MapReduce.
     *
     * Usage : hadoop jar FriendsCommon.jar FriendsCommon <input> <output>
     *
     * Format d'entrée attendu (fichier texte HDFS) :
     *   A    B,C,D
     *   B    A,C,E
     *   C    A,B,D
     *   D    A,C
     *   E    B
     *
     * @param args args[0] = répertoire HDFS d'entrée, args[1] = répertoire HDFS de sortie
     */
    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage : hadoop jar FriendsCommon.jar FriendsCommon <input> <output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Calcul des amis communs — Réseau Social");
        job.setJarByClass(FriendsCommon.class);

        // Mapper et Reducer
        job.setMapperClass(SocialGraphMapper.class);
        job.setReducerClass(CommonFriendsReducer.class);

        // Pas de Combiner ici : les listes d'amis ne sont pas agrégables localement
        // (on a besoin de TOUTES les listes pour calculer l'intersection)

        // Types de sortie du Mapper
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Types de sortie du Reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Formats d'entrée/sortie
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Nombre de Reducers (ajustable selon la taille du graphe)
        job.setNumReduceTasks(2);

        // Chemins HDFS
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.out.println("Lancement du job de calcul des amis communs...");
        System.out.println("Seuil de célébrité configuré : " + SEUIL_CELEBRITE + " amis");

        boolean succes = job.waitForCompletion(true);
        System.exit(succes ? 0 : 1);
    }
}