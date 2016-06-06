import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
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

    public static class FScorerMapper extends Mapper<Object, Text, WordPair, VerdictWritableArray> {
        String[] pos = { "america natives" , "circumstances ordinary" , "compelled give" , "great republicans" , "age present" , "dick growled" , "fishes north" , "one such" , "retorted sharply" , "data such" , "equal right" , "father left" , "figures hand" , "five years" , "promoting society" , "already flames" , "called child" , "former methods" , "instead suppose" , "irritated looked" , "john st" , "more years" , "scotts sir" , "came peter" , "county each" , "cut through" , "even national" , "new yorki" , "reason without" , "see thats" , "away tucked" , "based claim" , "big grunted" , "both works" , "contra see" , "em told" , "field two" , "fourteenth under" , "latter two" , "nations united" , "need spend" , "phenomena such" , "produce treatment" , "respect sex" , "above lived" , "bowman et" , "davis sammy" , "driven water" , "hours less" , "making statement" , "map north" , "natural rate" , "nights spent" , "problem somehow" , "turbulent waters" , "atheist hes" , "being means" , "biography current" , "brings map" , "closely connected" , "common species" , "day next" , "exerted influence" , "fort taking" , "man observed" , "paucity relative" , "provincial shall" , "social term" , "street way" , "always hes" , "always talking" , "bank case" , "body consumed" , "boston narrows" , "devastating more" , "ecole louvre" , "effects term" , "even though" , "few years" , "four miles" , "further nothing" , "graceful those" , "investigations preliminary" , "male white" , "man young" , "medicine psychosomatic" , "observers political" , "once released" , "pontiff sovereign" , "table vii" , "thing writhes" , "architectural genius" , "brief ln" , "cancer gastric" , "cent per" , "combination single" , "come messenger" , "day man" , "day mg" , "decision rent" , "disadvantage potential" , "emphasized men" , "exact locate" , "find good" , "front passed" , "good view" , "learning means" , "major toxicities" , "out wipes" , "perhaps telling" , "reason shows" , "abundantly present" , "addition another" , "american philosophy" , "asymptotically uniformly" , "back settled" , "bank second" , "better much" , "biological control" , "colin general" , "consumption function" , "context indicates" , "crowd weeping" , "cultured person" , "dancing re" , "down line" , "explanation offer" , "final result" , "national socialist" , "navigated way" , "oracle within" , "others saw" , "ring waldeyers" , "six thousand" , "stable uniformly" , "za zavod"};
        String[] neg = { "actors key" , "back going" , "intention primary" , "jacques monsieur" , "something think" , "appropriated congress" , "boy small" , "one such" , "came speak" , "economic relations" , "henry sir" , "never see" , "aircraft battery" , "bulb large" , "came minister" , "continues diary" , "difficulty raised" , "dorsal surface" , "home peter" , "limbs trunk" , "statement useful" , "competition refers" , "dated picture" , "duty legal" , "godfrey haller" , "joint routes" , "ll never" , "above data" , "above give" , "adequate survey" , "appended example" , "curve linear" , "external such" , "farmer small" , "followed party" , "friendly received" , "given statement" , "hardly seems" , "minutes three" , "notes one" , "remember seen" , "scene such" , "acts someone" , "aggregation kind" , "buck stove" , "cent per" , "context full" , "courtly love" , "east wind" , "factors undoubtedly" , "glasses rimmed" , "head under" , "infant premature" , "information up" , "jack uncle" , "lease signed" , "measurement negro" , "nearer one" , "pressure venous" , "qualities same" , "states united" , "tools useful" , "administrative costs" , "cut lets" , "defined now" , "door opened" , "four hundred" , "gas natural" , "geographical journal" , "leaves open" , "mg protein" , "one though" , "perhaps thought" , "preservation self" , "reinforcement used" , "announced document" , "appears upshot" , "approximately halfway" , "arms those" , "bacteria negative" , "before leaving" , "belongs law" , "biology comparative" , "both economic" , "cheung ming" , "chosen man" , "common risk" , "convinced sound" , "days two" , "during parliamentary" , "epithelium squamous" , "feet second" , "final takes" , "find something" , "include processes" , "leaving re" , "lived tribes" , "more one" , "paper put" , "prince wales" , "such times" , "addition causing" , "al et" , "authority weight" , "bernard george" , "change impulse" , "chapter next" , "chapter see" , "cocks shrill" , "colin powell" , "cyr et" , "deer north" , "difference key" , "different symbols" , "down passing" , "et roe" , "financial instability" , "first summer" , "followed river" , "footnote reads" , "form units" , "freely tears" , "goest thou" , "good public" , "here shall" , "hes thats" , "inch thirteen" , "integral number" , "laboratories use" , "literacy rates" , "loss weight" , "monyghams possible" , "more phrase" , "moved people" , "operating standard" , "probably send" , "problems special" , "savezni zavod" , "stepped ve" , "take things" };

        HashMap<String, Boolean> posPairs;
        HashMap<String, Boolean> negPairs;

        protected void setup(Context context) throws IOException, InterruptedException {
            posPairs = new HashMap<String, Boolean>();
            negPairs = new HashMap<String, Boolean>();

            for (String str : pos)
                posPairs.put(str, true);

            for (String str : neg)
                negPairs.put(str, true);
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            long tp, fp, fn;
            double precision, recall, F;

            for (int i = 0; i < THRESHOLD_TESTS_RESOLUTION; i++) {
                tp = context.getCounter(VERDICT_COUNTERS, Verdict.TRUE_POSITIVE.name() + "_" + (i + 1)).getValue();
//                tn = context.getCounter(VERDICT_COUNTERS, Verdict.TRUE_NEGATIVE.name() + "_" + (i + 1)).getValue();
                fp = context.getCounter(VERDICT_COUNTERS, Verdict.FALSE_POSITIVE.name() + "_" + (i + 1)).getValue();
                fn = context.getCounter(VERDICT_COUNTERS, Verdict.FALSE_NEGATIVE.name() + "_" + (i + 1)).getValue();
//                na = context.getCounter(VERDICT_COUNTERS, Verdict.NA.name() + "_" + (i + 1)).getValue();

                precision = tp + fp != 0 ? (double) tp / (tp + fp) : 1;
                recall    = tp + fn != 0 ? (double) tp / (tp + fn) : 1;
                F = 2.0 * precision * recall / (precision + recall);

                System.out.println(String.format("Threshold: %.2f, F: %f", (i + 1) * TESTS_THRESHOLD_GAP, F));
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] keyValue = value.toString().split("\t");
            String[] decadeCarCdr = keyValue[0].split(" +");

            int decade = Integer.parseInt(decadeCarCdr[0]);
            String car = decadeCarCdr[1];
            String cdr = decadeCarCdr[2];
            String pair = car + " " + cdr;
            double pmi = Double.parseDouble(keyValue[1]);

            VerdictWritableArray verdicts = new VerdictWritableArray(THRESHOLD_TESTS_RESOLUTION, pmi);
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

            context.write(new WordPair(car, cdr, decade), verdicts);
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
        fScorerJob.setNumReduceTasks(0);
        fScorerJob.setOutputKeyClass(WordPair.class);
        fScorerJob.setOutputValueClass(VerdictWritableArray.class);
        FileInputFormat.addInputPath(fScorerJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(fScorerJob, new Path(args[1]));

        fScorerJob.waitForCompletion(true);

        System.exit(0);
    }
}
