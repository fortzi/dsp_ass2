import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
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

        if(args.length < 3) {
            System.out.println("please enter input output1 output 2");
            return;
        }

        /**********************************************************************************/
        /* step one configuration and job setup */
        /**********************************************************************************/

        Configuration stepOneConf = new Configuration();
        ControlledJob stepOneJob = new ControlledJob(stepOneConf);
        stepOneJob.setJob(Job.getInstance(stepOneConf,"Step one"));

        stepOneJob.getJob().setJarByClass(StepOne.class);
        stepOneJob.getJob().setMapperClass(StepOne.StepOneMapper.class);
        stepOneJob.getJob().setReducerClass(StepOne.StepOneReducer.class);
        stepOneJob.getJob().setPartitionerClass(StepOne.StepOnePartitioner.class);
        stepOneJob.getJob().setMapOutputKeyClass(CarAndDecadeAndOrder.class);
        stepOneJob.getJob().setMapOutputValueClass(CountAndCdrAndPairCount.class);
        stepOneJob.getJob().setOutputKeyClass(WordPair.class);
        stepOneJob.getJob().setOutputValueClass(ThreeSums.class);
        stepOneJob.getJob().setInputFormatClass(SequenceFileInputFormat.class);

        FileInputFormat.addInputPath(stepOneJob.getJob(), new Path(args[0]));
        FileOutputFormat.setOutputPath(stepOneJob.getJob(), new Path(args[1]));

        /**********************************************************************************/
        /* step one configuration and job setup */
        /**********************************************************************************/

        Configuration stepTwoConf = new Configuration();
        ControlledJob stepTwoJob = new ControlledJob(stepTwoConf);
        stepTwoJob.setJob(Job.getInstance(stepTwoConf,"Step Two"));

        stepTwoJob.getJob().setJarByClass(StepTwo.class);
        stepTwoJob.getJob().setMapperClass(StepTwo.StepTwoMapper.class);
        stepTwoJob.getJob().setReducerClass(StepTwo.StepTwoReducer.class);
        stepTwoJob.getJob().setOutputKeyClass(WordPair.class);
        stepTwoJob.getJob().setOutputValueClass(ThreeSums.class);

        FileInputFormat.addInputPath(stepTwoJob.getJob(), new Path(args[1]));
        FileOutputFormat.setOutputPath(stepTwoJob.getJob(), new Path(args[2]));

        /**********************************************************************************/
        /* RUNNING ! */
        /**********************************************************************************/

        stepTwoJob.addDependingJob(stepOneJob);
        JobControl jc = new JobControl("JC");
        jc.addJob(stepOneJob);
        jc.addJob(stepTwoJob);

        new Thread(jc).run();

        while (!jc.allFinished()) {
            System.out.println("Still running...");
            Thread.sleep(5000);
        }

    }
}
