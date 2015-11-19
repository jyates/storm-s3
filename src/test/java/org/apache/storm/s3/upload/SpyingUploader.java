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

import org.apache.http.concurrent.FutureCallback;
import org.apache.storm.guava.io.ByteStreams;
import org.apache.storm.guava.util.concurrent.Futures;
import org.apache.storm.guava.util.concurrent.ListenableFuture;
import org.apache.storm.s3.output.upload.Uploader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.ObjectMetadata;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Uploader that just stores the 'files' in memory. Acts as a <b>blocking uploader</b> for each file
 */
public class SpyingUploader extends Uploader {
    private static final Logger LOG = LoggerFactory.getLogger(SpyingUploader.class);
    private static Map<String, Map<String, List<Upload>>> namespacedUploads = new
          ConcurrentHashMap<>();

    private Map<String, List<Upload>> uploads;
    private String nameSpace;
    private Uploader delegate;
    private static Map<String, Boolean> blocks;

    public void withDelegate(Uploader delegate) {
        this.delegate = delegate;
    }

    @Override
    public void prepare(Map conf) {
        uploads = new HashMap<>();
        blocks = new ConcurrentHashMap<>();
        namespacedUploads.put(nameSpace, uploads);
        delegate.prepare(conf);
    }

    @Override
    public void ensureBucketExists(String bucket) {
        delegate.ensureBucketExists(bucket);
    }

    @Override
    public ListenableFuture<Void> upload(String bucketName, String name, InputStream input,
          ObjectMetadata meta) throws IOException {
        if (blocks.get(this.nameSpace) != null) {
            return Futures.immediateFuture(null);
        }

        // copy the stream
        List<Upload> files = this.uploads.get(bucketName);
        if (files == null) {
            files = new ArrayList<>();
            this.uploads.put(bucketName, files);
        }
        Upload up = new Upload(name, input, meta);
        files.add(up);
        // send the stream on to the delegate
        ListenableFuture<Void> result =
              delegate.upload(bucketName, name, new ByteArrayInputStream(up.object), meta);
        // add the file once the upload is finished
        Futures.addCallback(result,
              new org.apache.storm.guava.util.concurrent.FutureCallback<Void>() {
                  @Override
                  public void onSuccess(Void aVoid) {
                  }

                  @Override
                  public void onFailure(Throwable throwable) {
                      up.fail();
                  }
              });
        return result;
    }

    public static Map<String, List<Upload>> getUploads(String nameSpace) {
        return namespacedUploads.get(nameSpace);
    }

    public static void waitForFileCount(String namespace, String bucketName, int fileCount)
          throws InterruptedException {
        //This is lazy - we could do this via a callback, but that means adding a listener and a
        // latch.
        while (true) {
            Map<String, List<Upload>> uploads = getUploads(namespace);
            if (uploads != null && uploads.size() == 1){
                List<Upload> files = uploads.get(bucketName);
                if(files != null && files.size() > fileCount) {
                    break;
                }
            }
                Thread.sleep(10);
        }
        LOG.info("Found at least {} files in {}/{}", fileCount, namespace, bucketName);
    }

    /**
     * Storm can be slow to close topologies, so block any more files being created in S3
     *
     * @param nameSpace
     */
    public static void block(String nameSpace) {
        blocks.put(nameSpace, true);
    }

    public void withNameSpace(String nameSpace) {
        this.nameSpace = nameSpace;
    }

    public class Upload {
        private String name;
        private byte[] object;
        private ObjectMetadata meta;
        private boolean failed;

        public Upload(String name, InputStream in, ObjectMetadata meta) throws IOException {
            assert meta.getContentLength() > 0;
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ByteStreams.copy(in, out);
            out.close();
            this.object = out.toByteArray();
            this.name = name;
            this.meta = meta;
        }

        @Override
        public String toString() {
            return "Upload{" +
                   "name='" + name + '\'' +
                   ", object=" + Arrays.toString(object) +
                   ", meta=" + meta +
                   '}';
        }

        public void fail() {
            this.failed = true;
        }
    }
}
