import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;

/**
 *
 * Created by doubled on 0005, 05, 6, 2016.
 */
public class Stepper {

    public enum COUNTERS {
        TOTAL_WORD_COUNT,
        KEY_VALUE_COUNT,
        MAPPERS_COUNT
    }
    public static final String WORD_COUNTERS = "WORD_COUNTERS";
    public static final String TOP_K = "TOP_K";

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if(args.length < 4) {
            System.out.println("please enter input output1 output2 k");
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

        for (Counter counter : stepOneJob.getCounters().getGroup(Stepper.WORD_COUNTERS))
            stepTwoConf.setLong(counter.getName(), counter.getValue());

        stepTwoConf.setInt(TOP_K, Integer.parseInt(args[3]));

        Job stepTwoJob = Job.getInstance(stepTwoConf, "Step Two");
        stepTwoJob.setJarByClass(StepTwo.class);
        stepTwoJob.setMapperClass(StepTwo.StepTwoMapper.class);
        stepTwoJob.setReducerClass(StepTwo.StepTwoReducer.class);
        stepTwoJob.setPartitionerClass(StepTwo.StepTwoPartitioner.class);
        stepTwoJob.setMapOutputKeyClass(WordPair.class);
        stepTwoJob.setMapOutputValueClass(ThreeSums.class);
        stepTwoJob.setOutputKeyClass(WordPair.class);
        stepTwoJob.setOutputKeyClass(DoubleWritable.class);
        FileInputFormat.addInputPath(stepTwoJob, new Path(args[1]));
        FileOutputFormat.setOutputPath(stepTwoJob, new Path(args[2]));

        stepTwoJob.waitForCompletion(true);

        /**********************************************************************************/
        /* printing info */
        /**********************************************************************************/

        System.out.println("##################################################");
        System.out.println("total key-value in each step:");
        System.out.println("StepOne: " + stepOneJob.getCounters().findCounter(COUNTERS.KEY_VALUE_COUNT).getValue());
        System.out.println("StepTwo: " + stepTwoJob.getCounters().findCounter(COUNTERS.KEY_VALUE_COUNT).getValue());
        System.out.println("##################################################");

        System.exit(0);
    }

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
}
