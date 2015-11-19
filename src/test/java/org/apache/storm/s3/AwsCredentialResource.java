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
package org.apache.storm.s3;

import org.junit.rules.ExternalResource;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.transfer.TransferManager;

/**
 * Resource to load AWS credentials and get clients to desired services
 */
public class AwsCredentialResource extends ExternalResource {

    private AmazonS3Client client;
    private TransferManager tx;

    @Override
    protected void before() throws Throwable {
        super.before();
        AWSCredentialsProvider provider = new ProfileCredentialsProvider("aws-testing");
        ClientConfiguration config = new ClientConfiguration();
        client = new AmazonS3Client(provider.getCredentials(), config);
        tx = new TransferManager(client);
    }

    public AmazonS3Client getClient() {
        return this.client;
    }

    public TransferManager getTransferManager() {
        return this.tx;
    }
}
