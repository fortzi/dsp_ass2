import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Step;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class StepTwo {

    public static class StepTwoMapper extends Mapper<Object, Text, WordPair, ThreeSums>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] keyValue = value.toString().split("\t");
            String[] decadeCarCdr = keyValue[0].split(" +");
            String[] threeSums = keyValue[1].split(" +");

            int decade = Integer.parseInt(decadeCarCdr[0]);
            String car = decadeCarCdr[1];
            String cdr = decadeCarCdr[2];

            long carSum = Integer.parseInt(threeSums[0]);
            long cdrSum = Integer.parseInt(threeSums[1]);
            long pairCount = Integer.parseInt(threeSums[2]);

            context.write(new WordPair(car, cdr, decade),new ThreeSums(carSum, cdrSum, pairCount));
        }
    }

    public static class StepTwoReducer extends Reducer<WordPair, ThreeSums, WordPair, DoubleWritable> {

        public void reduce(WordPair key, Iterable<ThreeSums> values, Context context) throws IOException, InterruptedException {

            long carSum=0;
            long cdrSum=0;
            long pairSum=0;

            //second time
            for (ThreeSums val : values) {
                // getCarSum() will always return either 0 or the true value, so we use max to take
                // the true value, regardless of when it comes.
                carSum = Math.max(carSum, val.getCarSum());
                cdrSum = Math.max(cdrSum, val.getCarSum());
                pairSum += val.getPairSum();
            }

//            System.out.println("8===============================================D");
//            System.out.println("Total words: " + context.getConfiguration().get("TOTAL_WORDS"));
//            System.out.println("Key value: " + context.getConfiguration().get("KEY_VALUE"));
//            for (int i = 1900; i <= 2000; i += 10)
//                System.out.println("Decade " + i + " " + context.getConfiguration().get(Stepper.WORDS_COUNTERS + StepOne.StepOneMapper.getDecade(i).toString()));

            long N = Long.parseLong(context.getConfiguration().get("TOTAL_WORDS"));
            double pmi = Math.log(N * pairSum / (carSum * cdrSum));

//            System.out.println(key.getDecade() + ": " + key.getWord1() + " " + key.getWord2() + " " + pmi);
//            System.out.println("8===============================================D");
//            context.write(key, new ThreeSums(carSum, cdrSum, pairSum));
            context.write(key, new DoubleWritable(pmi));
        }
    }

    /*public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");

        job.setJarByClass(StepTwo.class);

        job.setMapperClass(StepTwoMapper.class);
        //job.setCombinerClass(Step2Reducer.class);
        job.setReducerClass(StepTwoReducer.class);

        job.setMapOutputKeyClass(WordPair.class);
        job.setMapOutputValueClass(ThreeSums.class);

        job.setOutputKeyClass(WordPair.class);
        job.setOutputValueClass(ThreeSums.class);

        //job.setInputFormatClass(SequenceFileInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }*/
}