import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;

import java.io.*;
import java.util.Random;

/**
 * Created by Ofer Caspi on 04/02/2016.
 *
 */
public class S3Helper {
    AmazonS3 s3 = null;
    Random randomizer;

    private static final String BUCKET_NAME = "dsp-ass2-emr";

    public S3Helper() {
        s3 = new AmazonS3Client();
        randomizer = new Random();
    }

    /**
     * Puts a file in the default bucket
     * @param folder The folder in which to put the file in.
     * @param file The file to put in S3.
     * @return The URL to the newly uploaded file.
     */
    public String putObject(Folders folder, File file) {
        String objectKey = folder + "/" + file.getName();
        s3.putObject(new PutObjectRequest(BUCKET_NAME, objectKey, file));

        return objectKey;
    }

    /**
     * Gets an object from the default bucket.
     * @param objectKey The object's key in S3.
     * @return The object's content as a string.
     * @throws AmazonClientException if the result input was unable to be processed
     */
    public S3Object getObject(String objectKey) throws AmazonClientException {
        return s3.getObject(new GetObjectRequest(BUCKET_NAME, objectKey));
    }

    public BufferedReader getLineReaderFromObject(S3Object object) throws IOException {
        return new BufferedReader(new InputStreamReader(object.getObjectContent()));
    }

    public void removeObject(String objectKey) {
        s3.deleteObject(BUCKET_NAME, objectKey);
    }

    /**
     * The available folders in the bucket.
     */
    public enum Folders {
        LOGS    { public String toString() { return "log"; } }
    }
}
