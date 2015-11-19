/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.s3.output;

import org.apache.storm.s3.format.DefaultFileNameFormat;
import org.apache.storm.s3.output.upload.PutRequestUploader;
import org.apache.storm.s3.output.upload.Uploader;

import org.junit.Test;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.transfer.TransferManager;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Requires ~/.aws/credentials file
 * <p/>
 * <p/>
 * [aws-testing]
 * aws_access_key_id=<ACCESS_KEY>
 * aws_secret_access_key=<SECRET_KEY>
 */
public class S3MemBufferedOutputStreamTest {

    @Test
    public void testStream() throws IOException {
        AWSCredentialsProvider provider = new ProfileCredentialsProvider("aws-testing");
        ClientConfiguration config = new ClientConfiguration();
        AmazonS3Client client = new AmazonS3Client(provider.getCredentials(), config);

        String bucketName = S3SFileUtils.getBucket(client);
        TransferManager tx = new TransferManager(client);
        Uploader uploader = new PutRequestUploader();
        uploader.setClient(tx.getAmazonS3Client());
        ContentEncoding encoding = ContentEncoding.NONE;
        OutputStream outputStream = new S3MemBufferedOutputStream(uploader, bucketName,
              new DefaultFileNameFormat().withPrefix("test"), "text/plain", encoding, "id", 0);
        S3SFileUtils.writeFile(bucketName, outputStream);
        S3SFileUtils.verifyFile(client, bucketName);
    }
}
