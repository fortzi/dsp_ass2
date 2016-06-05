import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "new method - step one");
        job.setJarByClass(StepOne.class);
        job.setMapperClass(StepOne.StepOneMapper.class);
        //job.setCombinerClass(IntSumReducer.class); //needed to be removed to have diffrente classe in map and reduce.
        job.setReducerClass(StepOne.StepOneReducer.class);
        job.setPartitionerClass(StepOne.StepOnePartitioner.class);

        job.setMapOutputKeyClass(CarAndDecadeAndOrder.class);
        job.setMapOutputValueClass(CountAndCdrAndPairCount.class);

        job.setOutputKeyClass(WordPair.class);
        job.setOutputValueClass(ThreeSums.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

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
