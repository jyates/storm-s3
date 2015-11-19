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
package org.apache.storm.s3.output.upload;

import org.apache.storm.guava.util.concurrent.ListenableFuture;
import org.apache.storm.guava.util.concurrent.SettableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.event.ProgressEvent;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 *
 */
public class NonBlockingTransferManagerUploader extends Uploader {

    private static final Logger LOG =
          LoggerFactory.getLogger(NonBlockingTransferManagerUploader.class);
    private TransferManager tx;

    public void prepare(Map conf) {
        super.prepare(conf);
        this.tx = new TransferManager(client);
    }

    @Override
    public ListenableFuture<Void> upload(String bucketName, String name, InputStream input,
          ObjectMetadata meta) throws IOException {
        SettableFuture<Void> future = SettableFuture.create();
        LOG.info("Starting upload, bucket={}, key={}", bucketName, name);
        final Upload up = tx.upload(bucketName, name, input, meta);
        up.addProgressListener((ProgressEvent progressEvent) -> {
            switch (progressEvent.getEventType()) {
                case CLIENT_REQUEST_FAILED_EVENT:
                    future.setException(
                          new IOException("Failed to complete upload after " +
                                          progressEvent.getBytesTransferred() + " bytes"));
                case CLIENT_REQUEST_SUCCESS_EVENT:
                    LOG.info("Upload completed, bucket={}, key={}", bucketName, name);
                    future.set(null);
            }
        });
        return future;
    }
}
