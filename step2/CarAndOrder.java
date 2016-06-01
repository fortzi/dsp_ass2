/**
 * Created by doubled on 0015, 15, 5, 2016.
 */
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

class CarAndOrder implements WritableComparable<CarAndOrder> {

    private String word;
    private Integer order;
    private int decade;

    public CarAndOrder() {
        word = null;
        order = 0;
        decade = -1;
    }

    public CarAndOrder(String word, int bool, int year) {
        this.word = word;
        this.order = bool;
        this.decade = year - (year % 10);
    }

    public CarAndOrder(String wordSerialization, int startIndex) {
        String[] toks = wordSerialization.split("\t");
        this.word = toks[startIndex];
        this.order = Integer.parseInt(toks[startIndex+1]);
        this.decade = Integer.parseInt(toks[startIndex+2]);
    }

    public String getWord() { return word; }
    public int getOrder() { return order; }
    public int getDecade() { return decade; }

    @Override
    public void readFields(DataInput in) throws IOException {
        word = in.readUTF();
        order = in.readInt();
        decade = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(word);
        out.writeInt(order);
        out.writeInt(decade);
    }

    @Override
    public int compareTo(CarAndOrder other) {

        if(this.decade < other.decade)
            return -1;

        if(this.decade > other.decade)
            return +1;

        // so the are from the same year

        if(word.compareTo(other.word) != 0)
            return word.compareTo(other.word);

        return order.compareTo(other.order);
    }

    @Override
    public String toString(){
        return decade + ": (" + word + ", " + order + ")";
    }

}
