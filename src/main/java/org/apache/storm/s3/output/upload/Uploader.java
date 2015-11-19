/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.s3.output.upload;


import org.apache.storm.guava.annotations.VisibleForTesting;
import org.apache.storm.guava.util.concurrent.ListenableFuture;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.BucketLifecycleConfiguration;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

public abstract class Uploader<T> implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(Uploader.class);

    protected AmazonS3 client;
    private Protocol protocol = Protocol.HTTPS;
    // use the same defaults as the ClientConfiguration to save extra 'is set' checks in prepare
    private int proxyPort = -1;
    private String proxyHost;
    private String endpoint;
    private boolean ephemeralBucket;


    /**
     * Setu up the AmazonS3 client and any last minute configurations
     * @param conf configuration to parse
     */
    public void prepare(Map conf){
        AWSCredentialsProvider provider =
              new AWSCredentialsProviderChain(
                    new DefaultAWSCredentialsProviderChain(),
                    new ProfileCredentialsProvider("aws-testing"));
        AWSCredentials credentials = provider.getCredentials();
        ClientConfiguration config = new ClientConfiguration().withProtocol(protocol);
        config.withProxyHost(proxyHost);
        config.withProxyPort(proxyPort);
        this.client = new AmazonS3Client(credentials, config);
        if(endpoint != null) {
            client.setEndpoint(endpoint);
        }
    }

    public Uploader withProtocol(Protocol protocol){
        this.protocol = protocol;
        return this;
    }

    public Uploader withProxyPort(int proxyPort){
        this.proxyPort = proxyPort;
        return this;
    }

    public Uploader withProxyHost(String host){
        this.proxyHost = host;
        return this;
    }

    public Uploader withEndpoint(String endpoint){
        this.endpoint = endpoint;
        return this;
    }


    /**
     * Upload the input stream to S3.
     * @param bucketName Bucket to upload to
     * @param name full name of the file
     * @param input stream to materialize as a file in S3
     * @param meta metadata about the request
     * @return a {@link ListenableFuture} for the completion of the download. The success/failure
     * of the upload is returned as the result
     * @throws IOException
     */
    public abstract ListenableFuture<Void> upload(String bucketName, String name,
          InputStream input, ObjectMetadata meta) throws IOException;

    /**
     * By default the key is ignored, but some implementations might need that information
     */
    public ListenableFuture<Void> upload(T key, String bucketName, String name, InputStream
          input, ObjectMetadata meta) throws IOException{
        return this.upload(bucketName, name, input, meta);
    }


    public void ensureBucketExists(String bucket) {
        if (!client.doesBucketExist(bucket)) {
            client.createBucket(bucket);
            LOG.info("Creating bucket {}", bucket);

            // if the bucket should delete files after a day, for testing
            if(ephemeralBucket){
                BucketLifecycleConfiguration.Rule expire = new BucketLifecycleConfiguration.Rule
                      ().withId("Expire test bucket").withExpirationInDays(1).withStatus
                      (BucketLifecycleConfiguration.ENABLED.toString());
                BucketLifecycleConfiguration configuration =
                      new BucketLifecycleConfiguration()
                            .withRules(Arrays.asList(expire));

                // Save configuration.
                client.setBucketLifecycleConfiguration(bucket, configuration);

            }
        }
    }

    @VisibleForTesting
    public void setClientForTesting(AmazonS3 amazonS3Client) {
        this.client =amazonS3Client;
    }

    @VisibleForTesting
    public void setEphemeralBucketForTesting(boolean retentionTime) {
        this.ephemeralBucket = retentionTime;
    }
}
