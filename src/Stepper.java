import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 *
 * Created by doubled on 0005, 05, 6, 2016.
 */
public class Stepper {

    public enum COUNTERS {
        TOTAL_WORD_COUNT,
        KEY_VALUE_COUNT
    }
    public static final String WORD_COUNTERS = "WORD_COUNTERS";

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if(args.length < 3) {
            System.out.println("please enter input output1 output 2");
            return;
        }

        /**********************************************************************************/
        /* step one configuration and job setup */
        /**********************************************************************************/

        Job stepOneJob = Job.getInstance(new Configuration(), "Step one");
        stepOneJob.setJarByClass(StepOne.class);
        stepOneJob.setMapperClass(StepOne.StepOneMapper.class);
        stepOneJob.setReducerClass(StepOne.StepOneReducer.class);
        stepOneJob.setPartitionerClass(StepOne.StepOnePartitioner.class);
        stepOneJob.setMapOutputKeyClass(CarDecadeOrder.class);
        stepOneJob.setMapOutputValueClass(CountCdrPairCount.class);
        stepOneJob.setOutputKeyClass(WordPair.class);
        stepOneJob.setOutputValueClass(ThreeSums.class);
        stepOneJob.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(stepOneJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(stepOneJob, new Path(args[1]));

        stepOneJob.waitForCompletion(true);

        /**********************************************************************************/
        /* step two configuration and job setup */
        /**********************************************************************************/

        Configuration stepTwoConf = new Configuration();

        stepTwoConf.setLong(COUNTERS.TOTAL_WORD_COUNT.name(), stepOneJob.getCounters().findCounter(COUNTERS.TOTAL_WORD_COUNT).getValue());
        stepTwoConf.setLong(COUNTERS.KEY_VALUE_COUNT.name(), stepOneJob.getCounters().findCounter(COUNTERS.KEY_VALUE_COUNT).getValue());
        for (Counter counter : stepOneJob.getCounters().getGroup(Stepper.WORD_COUNTERS))
            stepTwoConf.setLong(counter.getName(), counter.getValue());

        Job stepTwoJob = Job.getInstance(stepTwoConf, "Step Two");
        stepTwoJob.setJarByClass(StepTwo.class);
        stepTwoJob.setMapperClass(StepTwo.StepTwoMapper.class);
        stepTwoJob.setReducerClass(StepTwo.StepTwoReducer.class);
        stepTwoJob.setMapOutputKeyClass(WordPair.class);
        stepTwoJob.setMapOutputValueClass(ThreeSums.class);
        stepTwoJob.setOutputKeyClass(WordPair.class);
        stepTwoJob.setOutputKeyClass(DoubleWritable.class);
        FileInputFormat.addInputPath(stepTwoJob, new Path(args[1]));
        FileOutputFormat.setOutputPath(stepTwoJob, new Path(args[2]));

        stepTwoJob.waitForCompletion(true);

        System.exit(0);
    }
}
