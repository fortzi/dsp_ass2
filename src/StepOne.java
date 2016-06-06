import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

public class StepOne {

    public static class StepOneMapper extends Mapper<Object, Text, CarDecadeOrder, CountCdrPairCount>{

        private static ArrayList<String> stopwords = new ArrayList<String>(Arrays.asList("a","able","about","across","after","all","almost","also","am","among","an","and","any","are","as","at","be","because","been","but","by","can","cannot","could","dear","did","do","does","either","else","ever","every","for","from","get","got","had","has","have","he","her","hers","him","his","how","however","i","if","in","into","is","it","its","just","least","let","like","likely","may","me","might","most","must","my","neither","no","nor","not","of","off","often","on","only","or","other","our","own","rather","said","say","says","she","should","since","so","some","than","that","the","their","them","then","there","these","they","this","tis","to","too","twas","us","wants","was","we","were","what","when","where","which","while","who","whom","why","will","with","would","yet","you","your"));

        private static boolean isOK(String str) {
            return !stopwords.contains(str.toLowerCase()) && str.length() > 1;
        }

        public static Integer getDecade(int year) {
            return (year - (year % 10));
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] dbrow = value.toString().split("\t");
            String[] ngram = dbrow[0].split("\\s+");

            int year = Integer.parseInt(dbrow[1]);
            int occurrences = Integer.parseInt(dbrow[2]);

            // normalizing words.
            for(int i = 0; i < ngram.length; i++)
                ngram[i] = ngram[i].replaceAll("[^a-zA-Z]", "").toLowerCase();

            // according to assignment instructions:
            if(year < 1900)
                return;

            // dispatching one for each legal word for the count process (order = 0)
            for(String word : ngram) {
                if (isOK(word)) {
                    context.write(new CarDecadeOrder(year, word, 0), new CountCdrPairCount(occurrences, "", 0));
                    context.getCounter(Stepper.COUNTERS.TOTAL_WORD_COUNT).increment(occurrences);
                    context.getCounter(Stepper.COUNTERS.KEY_VALUE_COUNT).increment(1);
                    context.getCounter(Stepper.WORD_COUNTERS, getDecade(year).toString()).increment(occurrences);
                }
            }

            int pivot = ngram.length / 2;
            if (!isOK(ngram[pivot]))
                return;

            for (int i = 0; i < ngram.length; i++) {
                if (i == pivot)
                    continue;

                if (isOK(ngram[i])) {
                    context.write(new CarDecadeOrder(year, ngram[i], 1), new CountCdrPairCount(0, ngram[pivot], occurrences));
                    context.write(new CarDecadeOrder(year, ngram[pivot], 1), new CountCdrPairCount(0, ngram[i], 0));
                    context.getCounter(Stepper.COUNTERS.KEY_VALUE_COUNT).increment(2);
                }
            }
        }
    }

    public static class StepOnePartitioner extends Partitioner<CarDecadeOrder, CountCdrPairCount> {
        @Override
        public int getPartition(CarDecadeOrder car, CountCdrPairCount cdr, int numPartitions) {
            return car.getWord().charAt(0) % numPartitions;
        }
    }

    public static class StepOneReducer extends Reducer<CarDecadeOrder, CountCdrPairCount, WordPair, ThreeSums> {
        private String lastWord = "";
        private long carSum, cdrSum, lastWordSum;
        private String car, cdr;

        public void reduce(CarDecadeOrder key, Iterable<CountCdrPairCount> values, Context context)
                throws IOException, InterruptedException {

            if(key.getWord().equals(lastWord)) {
                for (CountCdrPairCount val : values) {
                    //the middle word will sometimes add itself with null to be counted
                    if(val.getWord().equals("")) {
                        continue;
                    }

                    if (key.getWord().compareTo(val.getWord()) < 0) {
                        carSum = lastWordSum;
                        cdrSum = 0;
                        car = key.getWord();
                        cdr = val.getWord();
                    } else {
                        carSum = 0;
                        cdrSum = lastWordSum;
                        car = val.getWord();
                        cdr = key.getWord();
                    }

                    context.write(new WordPair(car, cdr, key.getDecade()), new ThreeSums(carSum, cdrSum, val.getPairCount()));
                }
            } else {
                lastWordSum = 0;
                lastWord = key.getWord();

                for (CountCdrPairCount val : values)
                    lastWordSum += val.getCount();
            }
        }
    }
}