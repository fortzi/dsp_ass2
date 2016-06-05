import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

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

public class PairCount {

    public static class NgramMapper extends Mapper<Object, Text, WordPair, IntWritable>{

        private Text word = new Text();
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

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] dbrow = value.toString().split("\t");
            String[] ngram = dbrow[0].split("\\s+");

            int year = Integer.parseInt(dbrow[1]);

            for(String str : ngram)
                    str = str.replaceAll("[^a-zA-Z]","");

            //according to assignment instructions:
            if(year < 1900)
                return;

            IntWritable occurrences = new IntWritable(Integer.parseInt(dbrow[2]));

            switch(ngram.length) {
                case 2:
                    if(isOK(ngram[0]) && isOK(ngram[1]))
                        context.write(new WordPair(ngram[0], ngram[1], year), occurrences);
                    break;
                case 3:
                    if(isNotOK(ngram[1]))
                        break;
                    if(isOK(ngram[0]))
                        context.write(new WordPair(ngram[0], ngram[1], year), occurrences);
                    if(isOK(ngram[2]))
                        context.write(new WordPair(ngram[2], ngram[1], year), occurrences);
                    break;
                case 4:
                    if(isNotOK(ngram[1]))
                        break;
                    if(isOK(ngram[0]))
                        context.write(new WordPair(ngram[0], ngram[1], year), occurrences);
                    if(isOK(ngram[2]))
                        context.write(new WordPair(ngram[2], ngram[1], year), occurrences);
                    if(isOK(ngram[3]))
                        context.write(new WordPair(ngram[3], ngram[1], year), occurrences);
                    break;
                case 5:
                    if(isNotOK(ngram[2]))
                        break;
                    if(isOK(ngram[0]))
                        context.write(new WordPair(ngram[0], ngram[2], year), occurrences);
                    if(isOK(ngram[1]))
                        context.write(new WordPair(ngram[1], ngram[2], year), occurrences);
                    if(isOK(ngram[3]))
                        context.write(new WordPair(ngram[3], ngram[2], year), occurrences);
                    if(isOK(ngram[4]))
                        context.write(new WordPair(ngram[4], ngram[2], year), occurrences);
                    break;
                default:
                    break;
            }
        }
    }

    public static class IntSumReducer extends Reducer<WordPair,IntWritable, WordPair,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(WordPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "pair count");
        job.setJarByClass(PairCount.class);
        job.setMapperClass(NgramMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(WordPair.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}