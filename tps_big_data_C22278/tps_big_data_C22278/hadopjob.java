import java.io.IOException;
import java.util.*;

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

// CLASSE 1 : ErrorCount - Comptage des codes d'erreur dans les logs
class ErrorCount {

    // Mapper : extrait le code d'erreur (STATUS) de chaque ligne
    public static class ErrorMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text errorCode = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            
            String line = value.toString();
            String[] fields = line.split("\\|");
            
            // Format attendu : DATE | IP | URL | STATUS | SIZE
            if (fields.length >= 4) {
                String status = fields[3].trim();
                errorCode.set(status);
                context.write(errorCode, one);
            }
        }
    }

    // Reducer : additionne les occurrences pour chaque code d'erreur
    public static class ErrorReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    // Combiner : optimise le shuffle en additionnant localement
    public static class ErrorCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: ErrorCount <input_path> <output_path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Comptage des codes d'erreur");
        job.setJarByClass(ErrorCount.class);

        job.setMapperClass(ErrorMapper.class);
        job.setCombinerClass(ErrorCombiner.class);
        job.setReducerClass(ErrorReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


// CLASSE 2 : FriendsCommon - Amis communs entre utilisateurs

class FriendsCommon {

    // Mapper : émet pour chaque paire d'amis d'un utilisateur
    public static class FriendsMapper extends Mapper<LongWritable, Text, Text, Text> {
        
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            
            String line = value.toString();
            String[] parts = line.split(" -> ");
            
            if (parts.length != 2) return;
            
            String utilisateur = parts[0].trim();
            String[] amis = parts[1].split(",");
            
            // Pour chaque combinaison de deux amis, émettre (ami1, ami2) -> utilisateur
            for (int i = 0; i < amis.length; i++) {
                String ami1 = amis[i].trim();
                for (int j = i + 1; j < amis.length; j++) {
                    String ami2 = amis[j].trim();
                    // Trier les clés pour avoir une clé unique (petit, grand)
                    String cle;
                    if (ami1.compareTo(ami2) < 0) {
                        cle = ami1 + "," + ami2;
                    } else {
                        cle = ami2 + "," + ami1;
                    }
                    context.write(new Text(cle), new Text(utilisateur));
                }
            }
            
            // Gérer le cas où l'utilisateur a moins de 2 amis (rien à émettre)
        }
    }

    // Reducer : collecte les amis communs pour chaque paire
    public static class FriendsReducer extends Reducer<Text, Text, Text, Text> {
        
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            
            Set<String> amisCommums = new HashSet<String>();
            
            for (Text val : values) {
                amisCommums.add(val.toString());
            }
            
            // Formater la sortie
            String result = amisCommums.toString();
            context.write(key, new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: FriendsCommon <input_path> <output_path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Amis communs entre utilisateurs");
        job.setJarByClass(FriendsCommon.class);

        job.setMapperClass(FriendsMapper.class);
        job.setReducerClass(FriendsReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}