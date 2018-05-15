package aws;

import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AWSHandlerTest {

    private static final String BUCKET_NAME = "belyakbuck";

    @Test
    public void getBucketsList() {
        List<String> bucketsList =  AWSHandler.getBucketNames();
        assertEquals(1, bucketsList.size());
        assertEquals(BUCKET_NAME, bucketsList.get(0));
    }


    @Test
    public void getObjectsList() {
        List<String> objectNames =  AWSHandler.getObjectsList(BUCKET_NAME);
        System.out.println(objectNames);
    }

    @Test
    public void lsitObjectsWithPrefix() {
        List<String> objectNames =  AWSHandler.getObjectsListWithPrefix(BUCKET_NAME, "test1/test2");
        System.out.println(objectNames);
    }

    @Test
    public void listCommonPrefixes() {
        List<String> prefixes =  AWSHandler.getObjectsListWithPrefix(BUCKET_NAME, "test1/test2");
        System.out.println(prefixes);
    }


    @Test
    public void uploadAndGetFile() throws IOException {
        String fileNameToUpload = "src/test/resources/testFile.txt";
        String key = AWSHandler.uploadFile(BUCKET_NAME, fileNameToUpload);

        //save file from AWS
        String retrivedFileName = key;
        AWSHandler.downloadFile(BUCKET_NAME, retrivedFileName);

        //compare files
        try(
            FileInputStream fis1 = new FileInputStream(fileNameToUpload);
            FileInputStream fis2 = new FileInputStream(retrivedFileName)
        ) {
            int ch1;
            int ch2;

            do {
                ch1 = fis1.read();
                ch2 = fis2.read();
                assertTrue(ch1 == ch2);
            } while (ch1 > 0 && ch2 > 0);
            assertTrue(ch1 <= 0 && ch2 <= 0);
        }
    }
}
