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
import org.apache.storm.s3.format.RecordFormat;
import org.apache.storm.s3.format.S3OutputConfiguration;
import org.apache.storm.s3.output.upload.Uploader;
import org.apache.storm.s3.rotation.FileRotationPolicy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Constants;
import backtype.storm.tuple.Tuple;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

public class S3Output implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(S3Output.class);
    private final FileRotationPolicy fileRotation;
    private final AbstractFileNameFormat format;
    private final RecordFormat recordFormat;
    private final S3OutputConfiguration s3;
    private final Uploader uploader;
    private OutputStreamBuilder streamBuilder;

    private int rotation = 0;
    private S3MemBufferedOutputStream out;
    private String identifier;

    public S3Output(FileRotationPolicy rotationPolicy, AbstractFileNameFormat fileNameFormat,
          RecordFormat recordFormat, S3OutputConfiguration s3Info, Uploader uploader) {
        this.fileRotation = rotationPolicy;
        this.format = fileNameFormat;
        this.recordFormat = recordFormat;
        this.s3 = s3Info;
        this.uploader = uploader;
    }

    public S3Output withIdentifier(String identifier) {
        this.identifier = identifier;
        return this;
    }

    public void prepare(Map conf) throws IOException {
        LOG.info("Preparing S3 Output for bucket {}", s3.getBucket());
        uploader.ensureBucketExists(s3.getBucket());
        LOG.info("Prepared S3 Output for bucket {} ", s3.getBucket());
        this.streamBuilder = new OutputStreamBuilder(uploader, s3, identifier, format);
        createOutputFile();
    }

    public ListenableFuture write(Tuple tuple) throws IOException {
        boolean rotate = isTickTuple(tuple);
        if (!rotate) {
            if(LOG.isTraceEnabled()){
                LOG.trace("Got tuple: "+tuple);
            }
            byte[] bytes = recordFormat.format(tuple);
            out.write(bytes);
            rotate = fileRotation.mark(bytes.length);
        }else{
            LOG.info("Rotating file because of time tick!");
        }

        ListenableFuture<Void> status = null;
        if (rotate) {
            status = rotateOutputFile();
            fileRotation.reset();
        }
        return status;
    }

    private ListenableFuture<Void> rotateOutputFile() throws IOException {
        LOG.info("Rotating output file...");
        long start = System.currentTimeMillis();
        ListenableFuture<Void> committing = out.commit();
        this.rotation++;
        createOutputFile();
        long time = System.currentTimeMillis() - start;
        LOG.info("File rotation took {} ms.", time);
        return committing;
    }

    private void createOutputFile() throws IOException {
        this.out = this.streamBuilder.build(rotation++);
    }

    private boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
               && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }
}
