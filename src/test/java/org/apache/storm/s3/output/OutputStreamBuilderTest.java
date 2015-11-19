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

import org.apache.storm.s3.AwsCredentialResource;
import org.apache.storm.s3.S3DependentTests;
import org.apache.storm.s3.format.AbstractFileNameFormat;
import org.apache.storm.s3.format.DefaultFileNameFormat;
import org.apache.storm.s3.format.S3OutputConfiguration;
import org.apache.storm.s3.output.upload.PutRequestUploader;
import org.apache.storm.s3.output.upload.Uploader;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.function.Function;
import java.util.zip.GZIPInputStream;

/**
 * Test that we can upload a file from the stream builder that is either gzipped or regular
 */
@Category(S3DependentTests.class)
public class OutputStreamBuilderTest {

    @ClassRule
    private static AwsCredentialResource credentials = new AwsCredentialResource();
    private static final AbstractFileNameFormat fileNameFormat = new DefaultFileNameFormat()
          .withPrefix("test");

    @Test
    public void testNoEncoding() throws Exception {
        S3OutputConfiguration s3 = new S3OutputConfiguration();
        String bucket = write(s3);
        verify(bucket, input -> input);
    }

    @Test
    public void testGzipEncoding() throws Exception {
        S3OutputConfiguration s3 = new S3OutputConfiguration().withContentEncoding(ContentEncoding.GZIP);
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
        S3SFileUtils.verifyFile(credentials.getClient(), bucket, decode);
    }

    private String write(S3OutputConfiguration s3) throws IOException {
        String bucket = S3SFileUtils.getBucket(credentials.getClient());
        s3.setBucket(bucket);

        Uploader uploader = new PutRequestUploader();
        uploader.setClientForTesting(credentials.getTransferManager().getAmazonS3Client());
        OutputStreamBuilder builder = new OutputStreamBuilder(uploader, s3, "id", fileNameFormat);
        OutputStream out = builder.build(0);
        S3SFileUtils.writeFile(bucket, out);
        return bucket;
    }
}
