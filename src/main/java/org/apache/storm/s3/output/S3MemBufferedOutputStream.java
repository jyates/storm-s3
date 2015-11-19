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

import org.apache.storm.guava.util.concurrent.ListenableFuture;
import org.apache.storm.s3.format.AbstractFileNameFormat;
import org.apache.storm.s3.output.upload.Uploader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.ObjectMetadata;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.ExecutionException;

/**
 * OutputStream that buffers data in memory before writing it to S3
 */
public class S3MemBufferedOutputStream<T> extends OutputStream {

    private static final Logger LOG = LoggerFactory.getLogger(S3MemBufferedOutputStream.class);

    private final String bucketName;
    private final String contentType;
    private final AbstractFileNameFormat fileNameFormat;
    private final ByteArrayOutputStream outputStream;
    private final Uploader uploader;
    private final String identifier;
    private final int rotation;
    private final String suffix;
    private final OutputStream writingStream;

    public S3MemBufferedOutputStream(Uploader uploader, String bucketName,
          AbstractFileNameFormat fileNameFormat, String contentType, ContentEncoding encoding,
          String
                identifier, int rotationNumber) throws IOException {
        this.outputStream = new ByteArrayOutputStream();
        this.writingStream = encoding.encode(this.outputStream);
        this.suffix = encoding.getSuffix();

        this.uploader = uploader;
        this.bucketName = bucketName;
        this.fileNameFormat = fileNameFormat;

        this.contentType = contentType;
        this.identifier = identifier;
        this.rotation = rotationNumber;
    }

    @Override
    public void write(int b) throws IOException {
        outputStream.write(b);
    }

    @Override
    public void close() throws IOException {
        try {
            commit().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException(e);
        }
    }

    public int size() {
        return this.outputStream.size();
    }

    public ListenableFuture commit() throws IOException {
        String name = fileNameFormat.getName(null, identifier, rotation,
              System.currentTimeMillis()) + suffix;
        LOG.info("uploading {}/{} to S3", bucketName, name);
        writingStream.close();
        final byte[] buf = outputStream.toByteArray();
        try (InputStream input = new ByteArrayInputStream(buf)) {
            ObjectMetadata meta = new ObjectMetadata();
            meta.setContentType(contentType);
            meta.setContentLength(buf.length);
            return uploader.upload(null, bucketName, name, input, meta);
        }
    }
}
