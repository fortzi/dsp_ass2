import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by doubled on 0005, 05, 6, 2016.
 */
public class Stepper {

    public static enum COUNTERS {
        TOTAL_WORDS,
        KEY_VALUE
    }
    public static final String WORDS_COUNTERS = "WORDS_COUNTERS";

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        JobConf step1_conf = new JobConf(new Configuration());
        JobConf step2_conf = new JobConf(new Configuration());
        Job step1 = Job.getInstance(step1_conf, "step1");
        Job step2 = Job.getInstance(step2_conf, "step2");

        step1_conf.setJarByClass(StepOne.class);
        step1_conf.setMapperClass((Class<? extends Mapper>) StepOne.StepOneMapper.class);
        step1_conf.setReducerClass((Class<? extends Reducer>) StepOne.StepOneReducer.class);
        step1_conf.setMapOutputKeyClass(CarAndDecadeAndOrder.class);
        step1_conf.setMapOutputValueClass(CountAndCdrAndPairCount.class);
        step1_conf.setOutputKeyClass(WordPair.class);
        step1_conf.setOutputValueClass(ThreeSums.class);

        FileInputFormat.addInputPath(step1, new Path("/input"));
        FileOutputFormat.setOutputPath(step1, new Path("/output_1"));

        step2_conf.setJarByClass(StepTwo.class);
        step2_conf.setMapperClass((Class<? extends Mapper>) StepTwo.StepTwoMapper.class);
        step2_conf.setReducerClass((Class<? extends Reducer>) StepTwo.StepTwoReducer.class);
        step2_conf.setOutputKeyClass(WordPair.class);
        step2_conf.setOutputValueClass(ThreeSums.class);

        FileInputFormat.addInputPath(step2, new Path("/output_1"));
        FileOutputFormat.setOutputPath(step2, new Path("/output_2"));

        // STOPPED HERE

        int result = job.waitForCompletion(true) ? 0 : 1;

        System.out.println("########################### finish ###############################");
        System.out.println("########################### finish ###############################");

        System.out.println("Total Words in Corpus: " + job.getCounters().findCounter(COUNTERS.TOTAL_WORDS).getValue());
        System.out.println("Total KeyValues sent: " + job.getCounters().findCounter(COUNTERS.KEY_VALUE).getValue());

        for (Counter counter : job.getCounters().getGroup(WORDS_COUNTERS)) {
            System.out.println("  - " + counter.getName() + ": "+counter.getValue());
        }

    }

}
