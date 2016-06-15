/**
 *
 * Created by doubled on 0015, 15, 5, 2016.
 */

import org.apache.hadoop.fs.shell.Count;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class CountCdrPairCount implements Writable {

    private int count;
    private String word;
    private int pairCount;

    public CountCdrPairCount() {
        word = null;
        count = 0;
        pairCount = 0;
    }

    public CountCdrPairCount(int count, String word, int pairCount) {
        this.count = count;
        this.word = word;
        this.pairCount = pairCount;
    }

    public CountCdrPairCount(String wordSerialization, int startIndex) {
        String[] toks = wordSerialization.split("\t");
        this.count = Integer.parseInt(toks[startIndex]);
        this.word = toks[startIndex+1];
        this.pairCount = Integer.parseInt(toks[startIndex+2]);
    }

    public CountCdrPairCount set(int count, String word, int pairCount) {
        this.count = count;
        this.word = word;
        this.pairCount = pairCount;

        return this;
    }

    public int getCount() { return count; }
    public String getWord() { return word; }
    public int getPairCount() { return pairCount; }

    @Override
    public void readFields(DataInput in) throws IOException {
        count = in.readInt();
        word = in.readUTF();
        pairCount = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(count);
        out.writeUTF(word);
        out.writeInt(pairCount);
    }

    @Override
    public String toString(){
        return count + " " + word + " " + pairCount;
    }

}
