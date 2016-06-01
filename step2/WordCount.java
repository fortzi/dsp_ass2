import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    public static class Step2Mapper extends Mapper<Object, Text, CarAndOrder, CdrAndPairCount>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] keyValue = value.toString().split("\t");
            String[] decadeCarCdr = keyValue[0].split(" +");

            int decade = Integer.parseInt(decadeCarCdr[0]);
            String car = decadeCarCdr[1];
            String cdr = decadeCarCdr[2];

            int pairCount = Integer.parseInt(keyValue[1]);

            context.write(new CarAndOrder(car, 0, decade), new CdrAndPairCount(pairCount, cdr));
            context.write(new CarAndOrder(car, 1, decade), new CdrAndPairCount(pairCount, cdr));
            context.write(new CarAndOrder(cdr, 0, decade), new CdrAndPairCount(pairCount, car));
            context.write(new CarAndOrder(cdr, 1, decade), new CdrAndPairCount(pairCount, car));
        }
    }

    public static class Step2Reducer extends Reducer<CarAndOrder, CdrAndPairCount, WordPair, ThreeSums> {
        private IntWritable result = new IntWritable();
        private String lastCar = "";
        private int lastCarSum = 0;

        public void reduce(CarAndOrder key, Iterable<CdrAndPairCount> values, Context context) throws IOException, InterruptedException {

            int carSum, cdrSum;

            if(lastCar.equals(key.getWord())) {
                //second time
                for (CdrAndPairCount val : values) {
                    //TODO make this more efficient (one comparison instead of 3)
                    carSum = key.getWord().compareTo(val.getWord()) < 0 ? lastCarSum : 0 ;
                    cdrSum = key.getWord().compareTo(val.getWord()) < 0 ? 0 : lastCarSum ;

                    context.write(
                            new WordPair(key.getWord(),val.getWord(),key.getDecade()),
                            new ThreeSums(carSum, cdrSum, val.getCount()));
                }
            }
            else {
                //first time
                lastCarSum = 0;
                lastCar = key.getWord();

                for (CdrAndPairCount val : values) {
                    lastCarSum += val.getCount();
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(PairCount.class);
        job.setMapperClass(Step2Mapper.class);
        job.setCombinerClass(Step2Reducer.class);
        job.setReducerClass(Step2Reducer.class);
        job.setOutputKeyClass(CarAndOrder.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}