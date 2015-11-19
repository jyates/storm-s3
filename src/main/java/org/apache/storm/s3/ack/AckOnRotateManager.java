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

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import java.util.ArrayList;
import java.util.List;

/**
 * Only 'acks' tuples after they have been successfully committed.
 * <p>
 * Note that this should only be used with the a blocking {@link org.apache.storm.s3.output
 * .upload.Uploader}. Otherwise, the tuples that are ack'ed/failed will not match those in the
 * respective transfers
 * </p>
 */
public class AckOnRotateManager extends TupleAckManager {
    /**
     * List of outstanding tuples to ack
     */
    private List<Tuple> toAck;

    public AckOnRotateManager(){
        toAck = new ArrayList<>();
    }

    @Override
    public void handleAck(Tuple tuple, boolean committed) {
        if (!committed) {
            this.toAck.add(tuple);
            return;
        }
        this.toAck.forEach(this.collector::ack);
        this.toAck.clear();
        collector.ack(tuple);

    }

    @Override
    public void fail(Tuple tuple) {
        this.collector.fail(tuple);
        this.toAck.forEach(this.collector::fail);
        this.toAck.clear();
    }
}
