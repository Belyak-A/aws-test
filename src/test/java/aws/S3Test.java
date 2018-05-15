/*
 * Copyright 2016 - 2018 Stacksoft, Inc. All rights reserved.
 * This is Stacksoft proprietary and confidential material and its use
 * is subject to license terms.
 */

package aws;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

@Slf4j
public class S3Test {
    private static final String BUCKET_NAME = "belyakbuck";
    private static AmazonS3 s3Client0;
    private static AmazonS3 s3Client1;

    private static final Path TEMP_DIR = Paths.get("temp");
    private static final Path file0 = TEMP_DIR.resolve("file0");
    private static final Path file1 = TEMP_DIR.resolve("file1");

    public static void setUpClass() throws IOException {
        s3Client0 = AmazonS3ClientBuilder.defaultClient();
        s3Client1 = AmazonS3ClientBuilder.defaultClient();

        //delete data from test bucket
        ListObjectsV2Result result = s3Client0.listObjectsV2(BUCKET_NAME);
        List<S3ObjectSummary> objects = result.getObjectSummaries();
        List<String> keysInBucket = objects.stream().map(S3ObjectSummary::getKey).collect(toList());

        if (!keysInBucket.isEmpty()) {
            DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(BUCKET_NAME);
            deleteObjectsRequest.withKeys(keysInBucket.toArray(new String[]{}));
            s3Client0.deleteObjects(deleteObjectsRequest);
        }

        //generate test data
        FileUtils.deleteDirectory(TEMP_DIR.toFile());
        Files.createDirectory(TEMP_DIR);

        Files.createFile(file0);
        Files.write(file0, Stream.generate(() -> "0").limit(10000000).collect(toList()));

        Files.createFile(file1);
        Files.write(file1, Stream.generate(() -> "1").limit(10000000).collect(toList()));
    }


    public static void write0() {
        log.debug("write0 started at {}", new Date());
        s3Client0.putObject(BUCKET_NAME, "testFile", file0.toFile());

        log.debug("write0 finished at {}", new Date());
    }

    public static void write1() {
        log.debug("write1 started at {}", new Date());
        s3Client1.putObject(BUCKET_NAME, "testFile", file1.toFile());

        log.debug("write1 finished at {}", new Date());
    }

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        setUpClass();

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        Future<?> f0 = executorService.submit(S3Test::write0);
        Future<?> f1 = executorService.submit(S3Test::write1);

        f0.get();
        f1.get();

    }
}
