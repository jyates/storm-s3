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

import org.apache.storm.s3.format.AbstractFileNameFormat;
import org.apache.storm.s3.format.DefaultFileNameFormat;
import org.apache.storm.s3.format.S3Output;
import org.apache.storm.s3.output.upload.PutRequestUploader;
import org.apache.storm.s3.output.upload.Uploader;

import org.junit.BeforeClass;
import org.junit.Test;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.transfer.TransferManager;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.function.Function;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.assertEquals;

/**
 * Test that we can upload a file from the stream builder that is either gzipped or regular
 * <p>
 * Requires  ~/.aws/credentials file
 * <p/>
 * [aws-testing]
 * aws_access_key_id=<ACCESS_KEY>
 * aws_secret_access_key=<SECRET_KEY>
 * </p>
 * That same user/role also needs to be able to have S3 permissions:
 * <ol>
 * <li>CreateBucket</li>
 * <li>PutObject</li>
 * <li>DeleteBucket</li>
 * <li>DeleteObject</li>
 * </ol>
 */
public class OutputStreamBuilderTest {

    private static AmazonS3Client client;
    private static TransferManager tx;

    private static final AbstractFileNameFormat fileNameFormat = new DefaultFileNameFormat()
          .withPrefix("test");

    @BeforeClass
    public static void getCredentials() {
        AWSCredentialsProvider provider = new ProfileCredentialsProvider("aws-testing");
        ClientConfiguration config = new ClientConfiguration();
        client = new AmazonS3Client(provider.getCredentials(), config);
        tx = new TransferManager(client);
    }

    @Test
    public void testNoEncoding() throws Exception {
        S3Output s3 = new S3Output();
        String bucket = write(s3);
        verify(bucket, input -> input);
    }

    @Test
    public void testGzipEncoding() throws Exception {
        S3Output s3 = new S3Output().withContentEncoding(ContentEncoding.GZIP);
        String bucket = write(s3);
        verify(bucket, input -> {
            try {
                return new GZIPInputStream(input);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void verify(String bucket, Function<InputStream, InputStream> decode)
          throws IOException {
        S3SFileUtils.verifyFile(client, bucket, decode);
    }

    private String write(S3Output s3) throws IOException {
        String bucket = S3SFileUtils.getBucket(client);
        s3.setBucket(bucket);

        Uploader uploader = new PutRequestUploader(tx.getAmazonS3Client());
        OutputStreamBuilder builder = new OutputStreamBuilder(uploader, s3, "id", fileNameFormat);
        OutputStream out = builder.build(0);
        S3SFileUtils.writeFile(bucket, out);
        return bucket;
    }
}
