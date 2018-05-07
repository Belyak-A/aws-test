package aws;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;

import java.io.*;
import java.util.List;
import java.util.UUID;

import static java.util.stream.Collectors.toList;

public class AWSHandler {
    public static List<String> getBucketNames() {
        AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        List<Bucket> buckets = s3.listBuckets();
        return buckets.stream().map(Bucket::getName).collect(toList());
    }

    /**
     *
     * @param fileNameToUpload
     * @return S3 key of uploaded file
     */
    public static String uploadFile(String bucketName, String fileNameToUpload) {
        System.out.format("Uploading %s to S3 bucket %s...\n", fileNameToUpload, bucketName);
        String fileKey = UUID.randomUUID().toString();
        final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        try {
            s3.putObject(bucketName, fileKey, new File(fileNameToUpload));
            return fileKey;
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            return null;
        }
    }

    public static void downloadFile(String bucketName, String key) {
        final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        try {
            S3Object o = s3.getObject(bucketName, key);
            S3ObjectInputStream s3is = o.getObjectContent();
            FileOutputStream fos = new FileOutputStream(new File(key));
            byte[] read_buf = new byte[1024];
            int read_len = 0;
            while ((read_len = s3is.read(read_buf)) > 0) {
                fos.write(read_buf, 0, read_len);
            }
            s3is.close();
            fos.close();
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        } catch (FileNotFoundException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    public static List<String> getObjectsList(String bucketName) {
        final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        ListObjectsV2Result result = s3.listObjectsV2(bucketName);
        List<S3ObjectSummary> objects = result.getObjectSummaries();
        for (S3ObjectSummary os: objects) {
            System.out.println("* " + os.getKey());
        }
        return objects.stream().map(S3ObjectSummary::getKey).collect(toList());
    }

    public static List<String> getObjectsListWithPrefix(String bucketName, String prefix) {
        final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
        ListObjectsV2Result result = s3.listObjectsV2(bucketName, prefix);
        List<S3ObjectSummary> objects = result.getObjectSummaries();
        for (S3ObjectSummary os: objects) {
            System.out.println("* " + os.getKey());
        }
        return objects.stream().map(S3ObjectSummary::getKey).collect(toList());
    }
}
