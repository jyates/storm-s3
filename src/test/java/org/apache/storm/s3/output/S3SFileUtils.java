/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.s3.output;

import org.apache.storm.s3.format.*;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;

/**
 * Utilities for helping to interact with S3
 */
public class S3SFileUtils {

    public static String writeFile(String bucket, OutputStream out) throws IOException {
        OutputStreamWriter writer = new OutputStreamWriter(out);
        PrintWriter printer = new PrintWriter(writer);
        printer.println("line1");
        printer.println("line2");
        printer.close();
        return bucket;
    }

    public static void verifyFile(AmazonS3Client client, String bucket) throws IOException {
        verifyFile(client, bucket, input -> input);
    }

    public static void verifyFile(AmazonS3Client client, String bucket, Function<InputStream,
          InputStream> decode)
          throws IOException {
        ObjectListing objectListing = client.listObjects(bucket);

        List<S3ObjectSummary> objectSummaries = objectListing.getObjectSummaries();
        assertEquals(1, objectSummaries.size());
        S3ObjectSummary s3ObjectSummary = objectSummaries.get(0);
        InputStreamReader reader = new InputStreamReader(decode.apply(
              client.getObject(bucket, s3ObjectSummary.getKey()).getObjectContent()));
        BufferedReader r = new BufferedReader(reader);
        assertEquals("line1", r.readLine());
        assertEquals("line2", r.readLine());
        client.deleteObject(bucket, s3ObjectSummary.getKey());
        client.deleteBucket(bucket);
    }

    public static String getBucket(AmazonS3Client client) {
        String bucketName = "test-bucket-" + UUID.randomUUID();
        client.createBucket(bucketName);
        return bucketName;
    }
}
