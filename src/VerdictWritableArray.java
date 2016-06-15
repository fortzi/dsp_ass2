import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * Created by user1 on 06/06/2016.
 */
public class VerdictWritableArray implements Writable {

    FScorer.Verdict[] verdicts;

    public VerdictWritableArray() {

    }

    public VerdictWritableArray(int size) {
        this.verdicts = new FScorer.Verdict[size];
    }

    public void setVerdict(int i, FScorer.Verdict v) {
        if (i >= verdicts.length || i < 0)
            throw new ArrayIndexOutOfBoundsException();

        verdicts[i] = v;
    }

    public FScorer.Verdict getVerdict(int i) {
        if (i >= verdicts.length || i < 0)
            throw new ArrayIndexOutOfBoundsException();

        return verdicts[i];
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        String[] out = dataInput.readUTF().split(" ");
        verdicts = new FScorer.Verdict[out.length];

        for (int i = 0; i < out.length; i++)
            verdicts[i] = FScorer.Verdict.valueOf(out[i]);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String[] temp = new String[verdicts.length];
        for (int i = 0; i < temp.length; i++)
            temp[i] = verdicts[i].name();

        out.writeUTF(String.join(" ", temp));
    }
}
