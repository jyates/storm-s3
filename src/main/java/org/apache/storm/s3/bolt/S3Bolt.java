/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.storm.guava.util.concurrent.ListenableFuture;
import org.apache.storm.s3.ack.AckOnRotateManager;
import org.apache.storm.s3.ack.TupleAckManager;
import org.apache.storm.s3.format.AbstractFileNameFormat;
import org.apache.storm.s3.format.DefaultFileNameFormat;
import org.apache.storm.s3.format.DelimitedRecordFormat;
import org.apache.storm.s3.format.RecordFormat;
import org.apache.storm.s3.format.S3OutputConfiguration;
import org.apache.storm.s3.output.S3Output;
import org.apache.storm.s3.output.upload.BlockingTransferManagerUploader;
import org.apache.storm.s3.output.upload.Uploader;
import org.apache.storm.s3.rotation.FileRotationPolicy;
import org.apache.storm.s3.rotation.FileSizeRotationPolicy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import java.io.IOException;
import java.util.Map;

public class S3Bolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(S3Bolt.class);

    private S3Output s3;
    private OutputCollector collector;

    // properties we can set and their defaults
    private FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(1.0F,
        FileSizeRotationPolicy.Units.MB);
    private AbstractFileNameFormat fileNameFormat = new DefaultFileNameFormat();
    private RecordFormat recordFormat = new DelimitedRecordFormat();
    private TupleAckManager ackManager = new AckOnRotateManager();
    private Uploader uploader = new BlockingTransferManagerUploader();

    // no defaults for the s3 information
    private S3OutputConfiguration s3Location;

    private int tickFrequencySecs = -1;

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        if(tickFrequencySecs > 0) {
            conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFrequencySecs);
        }
        return conf;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.ackManager.prepare(collector);

        // do any last preparation for the stored properties
        rotationPolicy.prepare(stormConf);
        fileNameFormat.prepare(stormConf);
        recordFormat.prepare(stormConf);
        s3Location.prepare(stormConf);
        uploader.prepare(stormConf);

        s3 = new S3Output(rotationPolicy, fileNameFormat,
              recordFormat, s3Location, uploader);
        String componentId = context.getThisComponentId();
        int taskId = context.getThisTaskId();
        s3.withIdentifier(componentId + "-" + taskId);

        // now that everything is ready, ensure we can reach s3 and have the bucket we need
        try {
            s3.prepare(stormConf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            ListenableFuture status = s3.write(tuple);
            this.ackManager.handleAck(tuple, status);
        } catch (IOException e) {
            LOG.warn("write/sync failed.", e);
            this.ackManager.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    public void setRotationPolicy(FileRotationPolicy rotationPolicy) {
        this.rotationPolicy = rotationPolicy;
    }

    public void setFileNameFormat(AbstractFileNameFormat fileNameFormat) {
        this.fileNameFormat = fileNameFormat;
    }

    public void setRecordFormat(RecordFormat recordFormat) {
        this.recordFormat = recordFormat;
    }

    public void setS3Location(S3OutputConfiguration s3Location) {
        this.s3Location = s3Location;
    }

    public void setTupleAckPolicy(TupleAckManager ackPolicy){
        this.ackManager = ackPolicy;
    }

    public void setUploader(Uploader uploader){
        this.uploader = uploader;
    }

    public void setTickFrequencySecs(int seconds){
        this.tickFrequencySecs = seconds;
    }
}
