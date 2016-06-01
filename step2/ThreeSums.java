/**
 * Created by doubled on 0015, 15, 5, 2016.
 */

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class ThreeSums implements Writable {

    private int carSum;
    private int cdrSum;
    private int pairSum;

    public ThreeSums() {
        carSum = 0;
        cdrSum = 0;
        pairSum = 0;
    }

    public ThreeSums(int car, int cdr, int pair) {
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

    public int getCarSum() { return carSum; }

    public int getCdrSum() { return cdrSum; }

    public int getPairSum() { return pairSum; }

    @Override
    public void readFields(DataInput in) throws IOException {
        carSum = in.readInt();
        cdrSum = in.readInt();
        pairSum = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(carSum);
        out.writeInt(cdrSum);
        out.writeInt(pairSum);
    }

    @Override
    public String toString(){
        return carSum + "," + cdrSum + "," + pairSum;
    }

}
