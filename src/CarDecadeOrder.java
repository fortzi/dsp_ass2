/**
 *
 * Created by doubled on 0015, 15, 5, 2016.
 */

import com.sun.istack.internal.NotNull;
import org.apache.commons.lang.NullArgumentException;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class CarDecadeOrder implements WritableComparable<CarDecadeOrder> {

    private String word;
    private int decade;
    private int order;

    public CarDecadeOrder() {
        word = null;
        decade = -1;
        order = 0;

    }

    public CarDecadeOrder(int year, String word, int order) {
        this.word = word;
        this.decade = year - (year % 10);
        this.order = order;
    }

    public CarDecadeOrder(String wordSerialization, int startIndex) {
        String[] toks = wordSerialization.split("\t");
        this.word = toks[startIndex];
        this.decade = Integer.parseInt(toks[startIndex+1]);
        this.order = Integer.parseInt(toks[startIndex+2]);
    }

    public CarDecadeOrder set(int year, String word, int order) {
        this.word = word;
        this.decade = year - (year % 10);
        this.order = order;

        return this;
    }

    public String getWord() { return word; }
    public int getDecade() { return decade; }
    public int getOrder() { return order; }

    @Override
    public void readFields(DataInput in) throws IOException {
        word = in.readUTF();
        decade = in.readInt();
        order = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(word);
        out.writeInt(decade);
        out.writeInt(order);
    }

    @Override
    public int compareTo(CarDecadeOrder other) {
        if (other == null)
            throw new NullArgumentException("other");

        if(this.decade < other.decade)
            return -1;

        if(this.decade > other.decade)
            return +1;

        // so the are from the same year
        if(word.compareTo(other.word) != 0)
            return word.compareTo(other.word);

        //0 1 -> -1
        //1 0 -> 1
        //0 0 -> 0
        //1 1 -> 0
        return order - other.order;
    }

    @Override
    public String toString(){
        return decade + " " + word + " " + order;
    }

}
