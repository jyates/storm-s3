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
package org.apache.storm.s3.bolt;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public class SentenceSpout extends BaseRichSpout {
    private ConcurrentHashMap<UUID, Values> pending;
    private SpoutOutputCollector collector;
    private String[] sentences = {
          "my dog has fleas",
          "i like cold beverages",
          "the dog ate my homework",
          "don't have a cow man",
          "i don't think i like fleas"
    };
    private int index = 0;
    private int count = 0;
    private long total = 0L;

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence", "timestamp"));
    }

    public void open(Map config, TopologyContext context,
          SpoutOutputCollector collector) {
        this.collector = collector;
        this.pending = new ConcurrentHashMap<UUID, Values>();
    }

    public void nextTuple() {
        Values values = new Values(sentences[index], System.currentTimeMillis());
        UUID msgId = UUID.randomUUID();
        this.pending.put(msgId, values);
        this.collector.emit(values, msgId);
        index++;
        if (index >= sentences.length) {
            index = 0;
        }
        count++;
        total++;
        if (count > 20000) {
            count = 0;
            System.out
                  .println("Pending count: " + this.pending.size() + ", total: " + this.total);
        }
        Thread.yield();
    }

    public void ack(Object msgId) {
        this.pending.remove(msgId);
    }

    public void fail(Object msgId) {
        System.out.println("**** RESENDING FAILED TUPLE");
        this.collector.emit(this.pending.get(msgId), msgId);
    }
}
