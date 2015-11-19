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
package org.apache.storm.s3.ack;

import org.apache.storm.guava.annotations.VisibleForTesting;
import org.apache.storm.guava.base.Preconditions;
import org.apache.storm.guava.util.concurrent.FutureCallback;
import org.apache.storm.guava.util.concurrent.Futures;
import org.apache.storm.guava.util.concurrent.ListenableFuture;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * An advanced version of the AckOnRotateManager that supports asynchronous upload to S3 via
 * multiple concurrently uploaders. Each ack request {@link #handleAck(Tuple, ListenableFuture)}
 * is checked for two things:
 * <ol>
 * <li>if an upload was started</li>
 * <li>the next manager to handle the request</li>
 * </ol>
 * If an upload was not started, the 'head' manager is removed from the queue and the tuple is
 * added to the manager before returning the manager to the head of the queue. If the tuple
 * was part of an upload that was started, then the 'head' manager is again taken from the queue,
 * but this time is added to a callback that listens for the completion of the upload. If the
 * upload succeeds (some time in the future) then the tuples handled by that manager are acked
 * back to the collector. Conversely, if the upload fails, those tuples are failed to the
 * collector.
 * <p>
 * Concurrency is managed because only one tuple/status pair is ever asked to be managed at one
 * time. This allows us to the freely take the 'head' manager and know that will always be the
 * head manager until the next status arrives.
 * </p>
 * By default, this class has an upload count of <tt>2</tt>. An upload count <tt>1</tt> will
 * cause this to act like an {@link AckOnRotateManager}, but will be slower due to queue locking
 * overhead. Similarly, it will also be slow if it is used with a blocking uploader like {@link
 * org.apache.storm.s3.output.upload.BlockingTransferManagerUploader} or {@link org.apache.storm
 * .s3.output.upload.PutRequestUploader}
 */
public class MultiConcurrentUploadAckManager extends TupleAckManager {

    private int uploadCount = 2;
    private LinkedBlockingDeque<AckOnRotateManager> uploaders;

    @Override
    public void prepare(OutputCollector collector) {
        super.prepare(collector);
        // setup the max number of uploaders we support
        this.uploaders = new LinkedBlockingDeque<>(uploadCount);
        for (int i = 0; i < uploadCount; i++) {
            AckOnRotateManager manager = new AckOnRotateManager();
            manager.prepare(collector);
            try {
                uploaders.put(manager);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void handleAck(final Tuple tuple, ListenableFuture<Void> committed)
          throws IOException {
        try {
            // wait for a manager to become available
            final AckOnRotateManager manager = this.uploaders.take();
            // this is a pending, unacked tuple.
            if (committed == null) {
                manager.handleAck(tuple, false);
                this.uploaders.offerFirst(manager);
                return;
            }
            // the current manager is 'done' do we take it out of the queue
            Futures.addCallback(committed, new FutureCallback<Void>() {
                @Override
                public void onSuccess(Void aBoolean) {
                    manager.handleAck(tuple, true);
                    uploaders.add(manager);
                }

                @Override
                public void onFailure(Throwable throwable) {
                    manager.fail(tuple);
                    uploaders.add(manager);
                }
            });
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }


    @Override
    public void fail(Tuple tuple) {
        try {
            AckOnRotateManager manager = this.uploaders.take();
            manager.fail(tuple);
            this.uploaders.add(manager);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void setConcurrentUploads(int concurrentUploads) {
        Preconditions.checkArgument(concurrentUploads > 0, "Must have a positive number of "
                                                           + " concurrent uploads");
        this.uploadCount = concurrentUploads;
    }

    @VisibleForTesting
    public LinkedBlockingDeque<AckOnRotateManager> getManagersForTesting() {
        return this.uploaders;
    }
}
