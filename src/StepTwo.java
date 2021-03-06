import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class StepTwo {

    public static class StepTwoMapper extends Mapper<Object, Text, WordPair, ThreeSums>{
        private WordPair newKey = new WordPair();
        private ThreeSums newValue = new ThreeSums();

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

            context.write(newKey.set(car, cdr, decade), newValue.set(carSum, cdrSum, pairCount));
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

        private Heaper heaper;
        private DoubleWritable newValue = new DoubleWritable();

        protected void setup(Reducer<WordPair, ThreeSums, WordPair, DoubleWritable>.Context context) throws IOException, InterruptedException {
            heaper = new Heaper(context.getConfiguration().getInt(Stepper.TOP_K, 0));
        }

        public void reduce(WordPair key, Iterable<ThreeSums> values, Context context) throws IOException, InterruptedException {

            long carSum = 0;
            long cdrSum = 0;
            long pairSum = 0;
            double pmi;

            for (ThreeSums val : values) {
                // getCarSum() will always return either 0 or the true value, so we use max to take
                // the true value, regardless of when it comes.
                carSum = Math.max(carSum, val.getCarSum());
                cdrSum = Math.max(cdrSum, val.getCdrSum());
                pairSum += val.getPairSum();
            }

            //if its the same word. one of the sums will be left empty .
            if(key.getWord1().equals(key.getWord2()))
                carSum = cdrSum = Math.max(carSum,cdrSum);

            long N = context.getConfiguration().getLong(String.valueOf(key.getDecade()), 0);
            pmi = Math.log((double)N * pairSum / (carSum * cdrSum));

            heaper.insert(key, pmi);

            newValue.set(pmi);
            context.write(key, newValue);
        }

        protected void cleanup(Reducer<WordPair, ThreeSums, WordPair, DoubleWritable>.Context context) throws IOException, InterruptedException {
            heaper.print();
        }
    }
}