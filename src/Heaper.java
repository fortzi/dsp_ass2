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
            pq.poll();
            pq.add(new PmiPair(pair, pmi));
        }

    }

    public void print() {
        for(Integer decade : db.keySet())
            for(PmiPair pair : db.get(decade))
                System.out.println(decade + ":" + pair.car + " " + pair.cdr + " - " + pair.pmi);

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
