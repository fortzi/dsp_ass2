import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

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
            context.getCounter(Stepper.COUNTERS.KEY_VALUE_COUNT).increment(1);
        }
    }

    public static class StepTwoPartitioner extends Partitioner<WordPair, ThreeSums> {
        @Override
        public int getPartition(WordPair key, ThreeSums value, int numPartitions) {
            return key.getDecade() % numPartitions;
        }
    }

    public static class StepTwoReducer extends Reducer<WordPair, ThreeSums, WordPair, DoubleWritable> {

        Heaper heaper;

        protected void setup(Reducer<WordPair, ThreeSums, WordPair, DoubleWritable>.Context context) throws IOException, InterruptedException {
            heaper = new Heaper(context.getConfiguration().getInt(Stepper.TOP_K,0));
        }

        public void reduce(WordPair key, Iterable<ThreeSums> values, Context context) throws IOException, InterruptedException {

            long carSum = 0;
            long cdrSum = 0;
            long pairSum = 0;

            for (ThreeSums val : values) {
                // getCarSum() will always return either 0 or the true value, so we use max to take
                // the true value, regardless of when it comes.
                carSum = Math.max(Math.max(carSum, val.getCarSum()), 1);
                cdrSum = Math.max(Math.max(cdrSum, val.getCarSum()), 1);
                pairSum += val.getPairSum();
            }

            long N = context.getConfiguration().getLong(String.valueOf(key.getDecade()), 1);
            double pmi = Math.log(N * pairSum / (carSum * cdrSum));

            heaper.insert(key, pmi);

            context.write(key, new DoubleWritable(pmi));
        }

        protected void cleanup(Reducer<WordPair, ThreeSums, WordPair, DoubleWritable>.Context context) throws IOException, InterruptedException {
            System.out.println("###################################");
            heaper.print();
            System.out.println("###################################");
        }
    }
}