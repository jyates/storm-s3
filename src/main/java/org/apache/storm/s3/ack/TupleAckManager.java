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

import org.apache.storm.guava.util.concurrent.ListenableFuture;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.ExecutionException;

/**
 * Manages the actual work of acking tuples back to the collector
 */
public abstract class TupleAckManager implements Serializable {

    protected OutputCollector collector;

    /**
     * Finish setting up the manager with the output collector
     *
     * @param collector to use when acking/failing tuples
     */
    public void prepare(OutputCollector collector) {
        this.collector = collector;
    }

    /**
     * @param committed if the file was committed to the output
     * @param tuple     tbe tuple to ack
     * @return <tt>true</tt> if this tuple should be acked
     */
    public void handleAck(Tuple tuple, ListenableFuture<Void> committed)
          throws IOException {
        try {
            boolean success = committed != null;
            if (success) {
                committed.get();
            }
            handleAck(tuple, success);
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException(e);
        }
    }

    protected void handleAck(Tuple tuple, boolean committedSuccessfully) {
        // noop - to be implemented to by simple ack managers
    }

    /**
     * Got a failure for the given tuple
     *
     * @param tuple
     */
    public abstract void fail(Tuple tuple);
}
