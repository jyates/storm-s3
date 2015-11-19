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
package org.apache.storm.s3.upload;

import org.apache.storm.guava.util.concurrent.Futures;
import org.apache.storm.guava.util.concurrent.ListenableFuture;
import org.apache.storm.s3.output.upload.Uploader;

import com.amazonaws.services.s3.model.ObjectMetadata;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Uploader that doesn't do any work
 */
public class NoOpUploader extends Uploader {
    @Override
    public void prepare(Map conf) {
        //noop
    }

    @Override
    public void ensureBucketExists(String bucket) {
        //noop
    }

    @Override
    public ListenableFuture<Void> upload(String bucketName, String name, InputStream input,
          ObjectMetadata meta) throws IOException {
        return Futures.immediateFuture(null);
    }
}
