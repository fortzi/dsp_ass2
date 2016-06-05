/**
 * Created by doubled on 0015, 15, 5, 2016.
 */

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class ThreeSums implements Writable {

    private long carSum;
    private long cdrSum;
    private long pairSum;

    public ThreeSums() {
        carSum = 0;
        cdrSum = 0;
        pairSum = 0;
    }

    public ThreeSums(long car, long cdr, long pair) {
        this.carSum = car;
        this.cdrSum = cdr;
        this.pairSum = pair;
    }

    public ThreeSums(String wordSerialization, int startIndex) {
        String[] toks = wordSerialization.split("\t");
        this.carSum = Integer.parseInt(toks[startIndex]);
        this.cdrSum = Integer.parseInt(toks[startIndex+1]);
        this.pairSum = Integer.parseInt(toks[startIndex+2]);
    }

    public long getCarSum() { return carSum; }

    public long getCdrSum() { return cdrSum; }

    public long getPairSum() { return pairSum; }

    @Override
    public void readFields(DataInput in) throws IOException {
        carSum = in.readLong();
        cdrSum = in.readLong();
        pairSum = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(carSum);
        out.writeLong(cdrSum);
        out.writeLong(pairSum);
    }

    @Override
    public String toString(){
        return carSum + " " + cdrSum + " " + pairSum;
    }

}
