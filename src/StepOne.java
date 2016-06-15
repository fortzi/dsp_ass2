import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

public class StepOne {

    public static class StepOneMapper extends Mapper<Object, Text, CarDecadeOrder, CountCdrPairCount>{

        private String[] stopwords = { "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren't", "as", "at", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can't", "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during", "each", "few", "for", "from", "further", "had", "hadn't", "has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", "let's", "me", "more", "most", "mustn't", "my", "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought", "our", "ours	ourselves", "out", "over", "own", "same", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such", "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there", "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "through", "to", "too", "under", "until", "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", "weren't", "what", "what's", "when", "when's", "where", "where's", "which", "while", "who", "who's", "whom", "why", "why's", "with", "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves" };
        private HashMap<String, Boolean> stopWordsHash;
        private CarDecadeOrder newKey = new CarDecadeOrder();
        private CountCdrPairCount newValue = new CountCdrPairCount();

        @Override
        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            stopWordsHash = new HashMap<String, Boolean>();
            for(String word : stopwords)
                stopWordsHash.put(word, null);
        }

        private boolean isOK(String str) {
            return str.length() > 1 && !stopWordsHash.containsKey(str);
        }

        public int getDecade(int year) {
            return (year - (year % 10));
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] dbrow = value.toString().split("\t");
            if(dbrow.length < 3) {
                return;
            }

            int year = Integer.parseInt(dbrow[1]);
            // according to assignment instructions:
            if(year < 1900) {
                return;
            }

            String[] ngram = dbrow[0].split("\\s+");

            String decade = String.valueOf(getDecade(year));
            int occurrences = Integer.parseInt(dbrow[2]);

            // normalizing words.
            for(int i = 0; i < ngram.length; i++) {
                ngram[i] = ngram[i].replaceAll("[^a-zA-Z]", "").toLowerCase();
                if (isOK(ngram[i])) {
                    context.write(newKey.set(year, ngram[i], 0), newValue.set(occurrences, "", 0));
                    context.getCounter(Stepper.COUNTERS.TOTAL_WORD_COUNT).increment(occurrences);
                    context.getCounter(Stepper.COUNTERS.KEY_VALUE_COUNT).increment(1);
                    context.getCounter(Stepper.WORD_COUNTERS, decade).increment(occurrences);
                }
            }

            int pivot = ngram.length / 2;
            if (!isOK(ngram[pivot]))
                return;

            for (int i = 0; i < ngram.length; i++) {
                if (i == pivot)
                    continue;

                if (isOK(ngram[i])) {
                    context.write(newKey.set(year, ngram[i], 1), newValue.set(0, ngram[pivot], occurrences));
                    context.write(newKey.set(year, ngram[pivot], 1), newValue.set(0, ngram[i], 0));
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
        private WordPair newKey = new WordPair();
        private ThreeSums newValue = new ThreeSums();

        public void reduce(CarDecadeOrder key, Iterable<CountCdrPairCount> values, Context context)
                throws IOException, InterruptedException {

            if(key.getWord().equals(lastWord)) {
                for (CountCdrPairCount val : values) {

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

                    context.write(newKey.set(car, cdr, key.getDecade()), newValue.set(carSum, cdrSum, val.getPairCount()));
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