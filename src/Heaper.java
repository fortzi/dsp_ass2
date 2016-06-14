import java.io.*;
import java.util.Hashtable;
import java.util.PriorityQueue;

/**
 * Created by doubled on 0005, 05, 6, 2016.
 */
public class Heaper {

    private Hashtable<Integer, PQ> db;
    private int topK;

    public Heaper(int k){
        db = new Hashtable<Integer, PQ>();
        topK = k;
    }

    public void insert(WordPair pair, double pmi) {
        if(!db.containsKey(pair.getDecade()))
            db.put(pair.getDecade(), new PQ());

        PQ pq = db.get(pair.getDecade());
        if(pq.size() < topK) {
            pq.add(new PmiPair(pair, pmi));
        } else if(pq.peek().pmi < pmi) {
            PmiPair old = pq.poll();
            old.car = pair.getWord1();
            old.cdr = pair.getWord2();
            old.pmi = pmi;
            pq.add(old);
        }

    }

    public void print() throws IOException {

        File resultsFile;
        PrintWriter results;

        resultsFile= File.createTempFile("topDecades-", ".txt");
        resultsFile.deleteOnExit();
        results = new PrintWriter(new BufferedWriter(new FileWriter(resultsFile.getPath(), true)));

        for(Integer decade : db.keySet())
            for(PmiPair pair : db.get(decade))
                results.println(decade + ":" + pair.car + " " + pair.cdr + " - " + pair.pmi);

        results.flush();
        results.close();

        //do not add empty file to s3
        //(this function will be called somtimes from reducer cleanup even when he didnt reduce anything)
        if(!db.isEmpty())
            new S3Helper().putObject(S3Helper.Folders.LOGS, resultsFile);
    }

    class PmiPair implements Comparable<PmiPair>{
        String car;
        String cdr;
        double pmi;

        public PmiPair(WordPair p, double pmi) {
            this.car = p.getWord1();
            this.cdr = p.getWord2();
            this.pmi = pmi;
        }

        @Override
        public int compareTo(PmiPair o) {
            double cmp =  this.pmi - o.pmi;
            if(cmp < 0) return -1;
            if(cmp > 0) return 1;
            return 0;
        }

        @Override
        public String toString() {
            return "PmiPair: " + car + " " + cdr + " " + pmi;
        }
    }

    class PQ extends PriorityQueue<PmiPair> {};

}
