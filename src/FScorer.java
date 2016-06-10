import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.Date;
import java.util.HashMap;

/**
 *
 * Created by user1 on 06/06/2016.
 */
public class FScorer {
    public static final int THRESHOLD_TESTS_RESOLUTION = 15;
    public static final double TESTS_THRESHOLD_GAP = (double) 10 / THRESHOLD_TESTS_RESOLUTION;
    public static final String VERDICT_COUNTERS = "VERDICT_COUNTERS";

    public enum Verdict {
        NA,
        TRUE_POSITIVE,
        TRUE_NEGATIVE,
        FALSE_POSITIVE,
        FALSE_NEGATIVE
    }

    public static class FScorerMapper extends Mapper<Object, Text, Text, Text> {
        String[] pos = { "tiger jaguar", "tiger feline", "closet clothes", "planet sun", "hotel reservation", "planet constellation", "credit card", "stock market", "psychology psychiatry", "planet moon", "planet galaxy", "bank money", "physics proton", "vodka brandy", "war troops", "Harvard Yale", "news report", "psychology Freud", "money wealth", "man woman", "FBI investigation", "network hardware", "nature environment", "seafood food", "weather forecast", "championship tournament", "law lawyer", "money dollar", "calculation computation", "planet star", "Jerusalem Israel", "vodka gin", "money bank", "computer software", "murder manslaughter", "king queen", "OPEC oil", "Maradona football", "mile kilometer", "seafood lobster", "furnace stove", "environment ecology", "boy lad", "asylum madhouse", "street avenue", "car automobile", "gem jewel", "type kind", "magician wizard", "football soccer", "money currency", "money cash", "coast shore", "money cash", "dollar buck", "journey voyage", "midday noon", "tiger tiger" };
        String[] neg = { "king cabbage", "professor cucumber", "chord smile", "noon string", "rooster voyage", "sugar approach", "stock jaguar", "stock life", "monk slave", "lad wizard", "delay racism", "stock CD", "drink ear", "stock phone", "holy sex", "production hike", "precedent group", "stock egg", "energy secretary", "month hotel", "forest graveyard", "cup substance", "possibility girl", "cemetery woodland", "glass magician", "cup entity", "Wednesday news", "direction combination", "reason hypertension", "sign recess", "problem airport", "cup article", "Arafat Jackson", "precedent collection", "volunteer motto", "listing proximity", "opera industry", "drink mother", "crane implement", "line insurance", "announcement effort", "precedent cognition", "media gain", "cup artifact", "Mars water", "peace insurance", "viewer serial", "president medal", "prejudice recognition", "drink car", "shore woodland", "coast forest", "century nation", "practice institution", "governor interview", "money operation", "delay news", "morality importance", "announcement production", "five month", "school center", "experience music", "seven series", "report gain", "music project", "cup object", "atmosphere landscape", "minority peace", "peace atmosphere", "morality marriage", "stock live", "population development", "architecture century", "precedent information", "situation isolation", "media trading", "profit warning", "chance credibility", "theater history", "day summer", "development issue" };


        HashMap<String, Boolean> posPairs;
        HashMap<String, Boolean> negPairs;

        private String sortWords(String str) {
            String[] words = str.split(" ");
            boolean w1After = words[0].compareTo(words[1]) > 0;
            return words[w1After ? 1 : 0] + " " + words[w1After ? 0 : 1];
        }

        protected void setup(Context context) throws IOException, InterruptedException {
            posPairs = new HashMap<String, Boolean>();
            negPairs = new HashMap<String, Boolean>();

            for (String str : pos)
                posPairs.put(sortWords(str), true);

            for (String str : neg)
                negPairs.put(sortWords(str), true);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] keyValue = value.toString().split("\t");
            String[] decadeCarCdr = keyValue[0].split(" +");

            int decade = Integer.parseInt(decadeCarCdr[0]);
            String car = decadeCarCdr[1];
            String cdr = decadeCarCdr[2];
            String pair = car + " " + cdr;
            double pmi = Double.parseDouble(keyValue[1]);

            VerdictWritableArray verdicts = new VerdictWritableArray(THRESHOLD_TESTS_RESOLUTION);
            boolean localVerdict;

            for (int i = 0; i < THRESHOLD_TESTS_RESOLUTION; i++) {
                localVerdict = pmi >= (i + 1) * TESTS_THRESHOLD_GAP;

                if (localVerdict && posPairs.containsKey(pair))
                    verdicts.setVerdict(i, Verdict.TRUE_POSITIVE);
                else if (localVerdict && negPairs.containsKey(pair))
                    verdicts.setVerdict(i, Verdict.FALSE_POSITIVE);
                else if (!localVerdict && posPairs.containsKey(pair))
                    verdicts.setVerdict(i, Verdict.FALSE_NEGATIVE);
                else if (!localVerdict && negPairs.containsKey(pair))
                    verdicts.setVerdict(i, Verdict.TRUE_NEGATIVE);
                else
                    verdicts.setVerdict(i, Verdict.NA);

                context.getCounter(VERDICT_COUNTERS, verdicts.getVerdict(i).name() + "_" + (i + 1)).increment(1);
            }
            //context.write(new Text("doron"), new Text("dalet"));
        }
    }

    public static class FScorerReducer extends Reducer<Text, Text, Text, Text> {

        public static void print() throws IOException {

            File resultsFile;
            PrintWriter results;

            resultsFile= File.createTempFile("test-", ".txt");
            resultsFile.deleteOnExit();
            results = new PrintWriter(new BufferedWriter(new FileWriter(resultsFile.getPath(), true)));

            results.printf("this is just a line for test 1");
            results.printf("this is just a line for test 2");
            results.printf("this is just a line for test 3");
            results.printf("this is just a line for test 4");

            results.flush();
            results.close();

            System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
            new S3Helper().putObject(S3Helper.Folders.LOGS, resultsFile);
            System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
        }

        Counters mapperCounters;

        protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            Cluster cluster = new Cluster(conf);
            Job currentJob = cluster.getJob(context.getJobID());
            mapperCounters = currentJob.getCounters();
        }


        /*protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            context.write(key, key);
        }*/

        protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            long tp, fp, fn;
            double precision, recall, F;
            File resultsFile;
            PrintWriter results;

            resultsFile= File.createTempFile("FScorer_", ".txt");
            resultsFile.deleteOnExit();
            results = new PrintWriter(new BufferedWriter(new FileWriter(resultsFile.getPath(), true)));

            for (int i = 0; i < THRESHOLD_TESTS_RESOLUTION; i++) {
                tp = mapperCounters.findCounter(VERDICT_COUNTERS, Verdict.TRUE_POSITIVE.name() + "_" + (i + 1)).getValue();
                // tn = context.getCounter(VERDICT_COUNTERS, Verdict.TRUE_NEGATIVE.name() + "_" + (i + 1)).getValue();
                fp = mapperCounters.findCounter(VERDICT_COUNTERS, Verdict.FALSE_POSITIVE.name() + "_" + (i + 1)).getValue();
                fn = mapperCounters.findCounter(VERDICT_COUNTERS, Verdict.FALSE_NEGATIVE.name() + "_" + (i + 1)).getValue();
                // na = context.getCounter(VERDICT_COUNTERS, Verdict.NA.name() + "_" + (i + 1)).getValue();

                precision = tp + fp != 0 ? (double) tp / (tp + fp) : 1;
                recall    = tp + fn != 0 ? (double) tp / (tp + fn) : 1;
                F = 2.0 * precision * recall / (precision + recall);

                System.out.printf("Threshold: %.2f, tp: %d, fp: %d, fn: %d\n", (i + 1) * TESTS_THRESHOLD_GAP, tp, fp, fn);
                results.printf("Threshold: %.2f, F: %f\n", (i + 1) * TESTS_THRESHOLD_GAP, F);
            }

            results.flush();
            results.close();

            new S3Helper().putObject(S3Helper.Folders.LOGS, resultsFile);
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length < 2) {
            System.out.println("please enter input and output paths");
            return;
        }

        Job fScorerJob = Job.getInstance(new Configuration(), "FScorer");
        fScorerJob.setJarByClass(FScorer.class);
        fScorerJob.setJobSetupCleanupNeeded(true);
        fScorerJob.setMapperClass(FScorer.FScorerMapper.class);
        fScorerJob.setReducerClass(FScorer.FScorerReducer.class);
        fScorerJob.setNumReduceTasks(1);
        fScorerJob.setOutputKeyClass(Text.class);
        fScorerJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(fScorerJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(fScorerJob, new Path(args[1]));

        fScorerJob.waitForCompletion(true);

        System.exit(0);
    }
}
