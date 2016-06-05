/**
 * Created by doubled on 0015, 15, 5, 2016.
 */
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

class WordPair implements WritableComparable<WordPair> {

    private String word1;
    private String word2;
    private int decade;

    public WordPair() {
        word1 = null;
        word2 = null;
        decade = -1;
    }

    public WordPair(String word1, String word2, int year) {
        this.word1 = word1;
        this.word2 = word2;
        this.decade = year - (year % 10);
    }

    public WordPair(String wordSerialization, int startIndex) {
        String[] toks = wordSerialization.split("\t");
        this.word1 = toks[startIndex];
        this.word2 = toks[startIndex+1];
        this.decade = Integer.parseInt(toks[startIndex+2]);
    }

    public String getWord1() { return word1; }
    public String getWord2() { return word2; }
    public int getDecade() { return decade; }

    @Override
    public void readFields(DataInput in) throws IOException {
        word1 = in.readUTF();
        word2 = in.readUTF();
        decade = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(word1);
        out.writeUTF(word2);
        out.writeInt(decade);
    }

    @Override
    public int compareTo(WordPair other) {

        if(this.decade < other.decade)
            return -1;

        if(this.decade > other.decade)
            return +1;

        // so the are from the same year

        if(word1.compareTo(other.word1) != 0)
            return word1.compareTo(other.word1);

        return word2.compareTo(other.word2);
    }

    @Override
    public String toString(){
        return decade + " " + word1 + " " + word2;
    }

}
