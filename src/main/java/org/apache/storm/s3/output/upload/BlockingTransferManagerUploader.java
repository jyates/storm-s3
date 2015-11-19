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
package org.apache.storm.s3.output.upload;


import org.apache.storm.guava.util.concurrent.ListenableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.ObjectMetadata;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutionException;

public class BlockingTransferManagerUploader extends NonBlockingTransferManagerUploader {

    private static final Logger LOG =
          LoggerFactory.getLogger(BlockingTransferManagerUploader.class);

    @Override
    public ListenableFuture<Void> upload(String bucketName, String name, InputStream input,
          ObjectMetadata meta)
          throws IOException {
        ListenableFuture<Void> future = super.upload(bucketName, name, input, meta);
        try {
            // block until the future completes
            future.get();
            return future;
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException(e);
        }
    }
}
