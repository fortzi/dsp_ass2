import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
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
        }
    }

    public static class StepTwoReducer extends Reducer<WordPair, ThreeSums, WordPair, DoubleWritable> {

        public void reduce(WordPair key, Iterable<ThreeSums> values, Context context) throws IOException, InterruptedException {

            long carSum = 0;
            long cdrSum = 0;
            long pairSum = 0;

            for (ThreeSums val : values) {
                // getCarSum() will always return either 0 or the true value, so we use max to take
                // the true value, regardless of when it comes.
                carSum = Math.max(carSum, val.getCarSum());
                cdrSum = Math.max(cdrSum, val.getCarSum());
                pairSum += val.getPairSum();
            }

            long N = context.getConfiguration().getLong(String.valueOf(key.getDecade()), 0);
            double pmi = Math.log(N * pairSum / (carSum * cdrSum));
            context.write(key, new DoubleWritable(pmi));
        }
    }
}