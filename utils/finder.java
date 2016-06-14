import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.sun.scenario.Settings;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by doubled on 0013, 13, 6, 2016.
 */
public class finder {

    private static final String BUCKET_NAME = "aws-logs-498180665878-us-east-1";

    private static void s3finder() {

        AmazonS3 s3 = new AmazonS3Client();
        DeleteObjectsRequest delReq = new DeleteObjectsRequest(BUCKET_NAME);
        ObjectListing listing;
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(BUCKET_NAME);

        do {
            listing = s3.listObjects(listObjectsRequest);
            System.out.printf("found %d files\n",listing.getObjectSummaries().size());
            for (S3ObjectSummary objectSummary : listing.getObjectSummaries()) {

                if (objectSummary.getKey().contains("stdout"))
                    System.out.println(objectSummary.getKey());
            }
            listObjectsRequest.setMarker(listing.getNextMarker());
        } while (listing.isTruncated());

        System.out.println("end");
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        s3finder();
    }

}
