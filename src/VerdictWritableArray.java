import com.sun.corba.se.spi.ior.Writeable;
import org.omg.CORBA_2_3.portable.OutputStream;

/**
 *
 * Created by user1 on 06/06/2016.
 */
public class VerdictWritableArray implements Writeable {

    double pmi;
    FScorer.Verdict[] verdicts;

    public VerdictWritableArray(int size, double pmi) {
        this.verdicts = new FScorer.Verdict[size];
        this.pmi = pmi;
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

    public double getPmi() {
        return pmi;
    }

    public void setPmi(double pmi) {
        this.pmi = pmi;
    }

    @Override
    public void write(OutputStream out) {
        String[] temp = new String[verdicts.length];
        for (int i = 0; i < temp.length; i++)
            temp[i] = verdicts[i].name();

        out.write_double(pmi);
        out.write_string(String.join(" ", temp));
    }
}
