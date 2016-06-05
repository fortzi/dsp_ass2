import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class StepOne {

    public static class StepOneMapper extends Mapper<Object, Text, CarAndDecadeAndOrder, CountAndCdrAndPairCount>{

        private static ArrayList<String> stopwords = new ArrayList<String>(Arrays.asList("a","able","about","across","after","all","almost","also","am","among","an","and","any","are","as","at","be","because","been","but","by","can","cannot","could","dear","did","do","does","either","else","ever","every","for","from","get","got","had","has","have","he","her","hers","him","his","how","however","i","if","in","into","is","it","its","just","least","let","like","likely","may","me","might","most","must","my","neither","no","nor","not","of","off","often","on","only","or","other","our","own","rather","said","say","says","she","should","since","so","some","than","that","the","their","them","then","there","these","they","this","tis","to","too","twas","us","wants","was","we","were","what","when","where","which","while","who","whom","why","will","with","would","yet","you","your"));

        private static boolean isOK(String str) {
            return !isStopWord(str) && isLegal(str);
        }

        private static boolean isNotOK(String str) {
            return !isOK(str);
        }

        private static boolean isStopWord(String str) {
            return stopwords.contains(str.toLowerCase());
        }

        private static boolean isLegal(String str) {
            return (str.length() > 1);
        }

        public static Integer getDecade(int year) {
            return (year - (year % 10));
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] dbrow = value.toString().split("\t");
            String[] ngram = dbrow[0].split("\\s+");

            int year = Integer.parseInt(dbrow[1]);
            int occurrences = Integer.parseInt(dbrow[2]);

            //normalizing words.
            for(int i=0; i<ngram.length; i++)
                ngram[i] = ngram[i].replaceAll("[^a-zA-Z]","").toLowerCase();

            //according to assignment instructions:
            if(year < 1900)
                return;

            //dispatching one for each legal word for the count process (order=0)
            for(String word : ngram)
                if(isOK(word)) {
                    context.write(new CarAndDecadeAndOrder(year, word, 0), new CountAndCdrAndPairCount(occurrences, "", 0));
                    context.getCounter(Stepper.COUNTERS.TOTAL_WORDS).increment(occurrences);
                    context.getCounter(Stepper.COUNTERS.KEY_VALUE).increment(1);
                    context.getCounter(Stepper.WORDS_COUNTERS,getDecade(year).toString()).increment(occurrences);
                }

            switch(ngram.length) {

                case 2:
                    if(isNotOK(ngram[1]))
                        break;
                    if(isOK(ngram[0])) {
                        context.write(new CarAndDecadeAndOrder(year, ngram[0], 1), new CountAndCdrAndPairCount(0, ngram[1], occurrences));
                        context.write(new CarAndDecadeAndOrder(year, ngram[1], 1), new CountAndCdrAndPairCount(0, ngram[0], 0));
                        context.getCounter(Stepper.COUNTERS.KEY_VALUE).increment(2);
                    }
                    break;

                case 3:
                    if(isNotOK(ngram[1]))
                        break;
                    if(isOK(ngram[0])) {
                        context.write(new CarAndDecadeAndOrder(year, ngram[0], 1), new CountAndCdrAndPairCount(0, ngram[1], occurrences));
                        context.write(new CarAndDecadeAndOrder(year, ngram[1], 1), new CountAndCdrAndPairCount(0, ngram[0], 0));
                        context.getCounter(Stepper.COUNTERS.KEY_VALUE).increment(2);
                    }
                    if(isOK(ngram[2])) {
                        context.write(new CarAndDecadeAndOrder(year, ngram[2], 1), new CountAndCdrAndPairCount(0,ngram[1],occurrences));
                        context.write(new CarAndDecadeAndOrder(year, ngram[1], 1), new CountAndCdrAndPairCount(0, ngram[2], 0));
                        context.getCounter(Stepper.COUNTERS.KEY_VALUE).increment(2);
                    }
                    break;

                case 4:
                    if(isNotOK(ngram[1]))
                        break;
                    if(isOK(ngram[0])) {
                        context.write(new CarAndDecadeAndOrder(year, ngram[0], 1), new CountAndCdrAndPairCount(0, ngram[1], occurrences));
                        context.write(new CarAndDecadeAndOrder(year, ngram[1], 1), new CountAndCdrAndPairCount(0, ngram[0], 0));
                        context.getCounter(Stepper.COUNTERS.KEY_VALUE).increment(2);
                    }
                    if(isOK(ngram[2])) {
                        context.write(new CarAndDecadeAndOrder(year, ngram[2], 1), new CountAndCdrAndPairCount(0,ngram[1],occurrences));
                        context.write(new CarAndDecadeAndOrder(year, ngram[1], 1), new CountAndCdrAndPairCount(0, ngram[2], 0));
                        context.getCounter(Stepper.COUNTERS.KEY_VALUE).increment(2);
                    }
                    if(isOK(ngram[3])) {
                        context.write(new CarAndDecadeAndOrder(year, ngram[3], 1), new CountAndCdrAndPairCount(0,ngram[1],occurrences));
                        context.write(new CarAndDecadeAndOrder(year, ngram[1], 1), new CountAndCdrAndPairCount(0, ngram[3], 0));
                        context.getCounter(Stepper.COUNTERS.KEY_VALUE).increment(2);
                    }
                    break;

                case 5:
                    if(isNotOK(ngram[2]))
                        break;
                    if(isOK(ngram[0])) {
                        context.write(new CarAndDecadeAndOrder(year, ngram[0], 1), new CountAndCdrAndPairCount(0,ngram[2],occurrences));
                        context.write(new CarAndDecadeAndOrder(year, ngram[2], 1), new CountAndCdrAndPairCount(0, ngram[0], 0));
                        context.getCounter(Stepper.COUNTERS.KEY_VALUE).increment(2);
                    }
                    if(isOK(ngram[1])) {
                        context.write(new CarAndDecadeAndOrder(year, ngram[1], 1), new CountAndCdrAndPairCount(0,ngram[2],occurrences));
                        context.write(new CarAndDecadeAndOrder(year, ngram[2], 1), new CountAndCdrAndPairCount(0, ngram[1], 0));
                        context.getCounter(Stepper.COUNTERS.KEY_VALUE).increment(2);
                    }
                    if(isOK(ngram[3])) {
                        context.write(new CarAndDecadeAndOrder(year, ngram[3], 1), new CountAndCdrAndPairCount(0,ngram[2],occurrences));
                        context.write(new CarAndDecadeAndOrder(year, ngram[2], 1), new CountAndCdrAndPairCount(0, ngram[3], 0));
                        context.getCounter(Stepper.COUNTERS.KEY_VALUE).increment(2);
                    }
                    if(isOK(ngram[4])) {
                        context.write(new CarAndDecadeAndOrder(year, ngram[4], 1), new CountAndCdrAndPairCount(0,ngram[2],occurrences));
                        context.write(new CarAndDecadeAndOrder(year, ngram[2], 1), new CountAndCdrAndPairCount(0, ngram[4], 0));
                        context.getCounter(Stepper.COUNTERS.KEY_VALUE).increment(2);
                    }
                    break;

                default:
                    break;
            }
        }
    }

    public static class StepOnePartitioner extends Partitioner<CarAndDecadeAndOrder, CountAndCdrAndPairCount> {
        @Override
        public int getPartition(CarAndDecadeAndOrder car, CountAndCdrAndPairCount cdr, int numPartitions) {
            return car.getWord().charAt(0) % numPartitions;
        }
    }

    public static class StepOneReducer extends Reducer<CarAndDecadeAndOrder,CountAndCdrAndPairCount, WordPair, ThreeSums> {
        private String lastWord = "";
        private long lastWordSum, carSum, cdrSum;

        public void reduce(CarAndDecadeAndOrder key, Iterable<CountAndCdrAndPairCount> values, Context context)
                throws IOException, InterruptedException {

            System.out.print(key + " ### (last: " + lastWord + ") "); //todo remove

            if(key.getWord().equals(lastWord)) {
                for (CountAndCdrAndPairCount val : values) {
                    System.out.print("["+val.getWord()+"]"); //todo remove
                    //the middle word will sometimes add itself with null to be counted
                    if(val.getWord().equals("")) {
                        System.out.print("!!!!!!!!!!!!!!!!!"); //todo remove
                        continue;
                    }

                    //TODO make this more efficient (one comparison instead of 3)
                    carSum = key.getWord().compareTo(val.getWord()) < 0 ? lastWordSum : 0;
                    cdrSum = key.getWord().compareTo(val.getWord()) < 0 ? 0 : lastWordSum;

                    context.write(
                                new WordPair(key.getWord(), val.getWord(), key.getDecade()),
                                new ThreeSums(carSum, cdrSum, val.getPairCount()));
                }
            }
            else {
                lastWordSum = 0;
                lastWord = key.getWord();
                for (CountAndCdrAndPairCount val : values) {
                    System.out.print("{"+val.getWord()+"}"); //todo remove
                    lastWordSum += val.getCount();
                }
            }

            System.out.println(""); //TODO remove
        }

    }

    /*public static void main(String[] args) throws Exception {

        System.out.println("########################### start ###############################");
        System.out.println("########################### start ###############################");

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "new method - step one");
        job.setJarByClass(StepOne.class);
        job.setMapperClass(StepOneMapper.class);
        //job.setCombinerClass(IntSumReducer.class); //needed to be removed to have diffrente classe in map and reduce.
        job.setReducerClass(StepOneReducer.class);
        job.setPartitionerClass(StepOnePartitioner.class);

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

        System.out.println("Total Words in Corpus: " + job.getCounters().findCounter(Stepper.COUNTERS.TOTAL_WORDS).getValue());
        System.out.println("Total KeyValues sent: " + job.getCounters().findCounter(Stepper.COUNTERS.KEY_VALUE).getValue());

        for (Counter counter : job.getCounters().getGroup(Stepper.WORDS_COUNTERS)) {
            System.out.println("  - " + counter.getName() + ": "+counter.getValue());
        }

        System.exit(result);
    }*/
}