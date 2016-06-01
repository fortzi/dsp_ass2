/**
 * Created by doubled on 0015, 15, 5, 2016.
 */

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class CdrAndPairCount implements Writable {

    private String word;
    private int count;

    public CdrAndPairCount() {
        word = null;
        count = 0;
    }

    public CdrAndPairCount(int count, String word) {
        this.word = word;
        this.count = count;
    }

    public CdrAndPairCount(String wordSerialization, int startIndex) {
        String[] toks = wordSerialization.split("\t");
        this.word = toks[startIndex];
        this.count = Integer.parseInt(toks[startIndex+1]);
    }

    public String getWord() { return word; }
    public int getCount() { return count; }

    @Override
    public void readFields(DataInput in) throws IOException {
        word = in.readUTF();
        count = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(word);
        out.writeInt(count);
    }

    @Override
    public String toString(){
        return word + "," + count;
    }

}
