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

import org.apache.storm.s3.format.AbstractFileNameFormat;
import org.apache.storm.s3.format.DefaultFileNameFormat;
import org.apache.storm.s3.format.DelimitedRecordFormat;
import org.apache.storm.s3.format.RecordFormat;
import org.apache.storm.s3.format.S3OutputConfiguration;
import org.apache.storm.s3.output.upload.BlockingTransferManagerUploader;
import org.apache.storm.s3.output.upload.Uploader;
import org.apache.storm.s3.rotation.FileRotationPolicy;
import org.apache.storm.s3.rotation.FileSizeRotationPolicy;

import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

import java.util.HashMap;
import java.util.Map;

public class S3Topology {
    static final String SENTENCE_SPOUT_ID = "sentence-spout";
    static final String BOLT_ID = "my-bolt";
    static final String TOPOLOGY_NAME = "test-topology";

    public static void main(String[] args) throws Exception {
        // setup the bolt
        S3Bolt bolt = new S3Bolt();
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(1.0F,
            FileSizeRotationPolicy.Units.MB);
        bolt.setRotationPolicy(rotationPolicy);
        AbstractFileNameFormat format = new DefaultFileNameFormat().withExtension(".txt")
                                                                   .withPrefix("test");
        bolt.setFileNameFormat(format);
        RecordFormat recordFormat = new DelimitedRecordFormat();
        bolt.setRecordFormat(recordFormat);

        Uploader uploader = new BlockingTransferManagerUploader();
        bolt.setUploader(uploader);

        S3OutputConfiguration s3 =
            new S3OutputConfiguration().setBucket("test-bucket")
                                  .setContentType("text/plain")
                                  .withPath("foo");
        bolt.setS3Location(s3);

        TopologyBuilder builder = new TopologyBuilder();

        SentenceSpout spout = new SentenceSpout();
        builder.setSpout(SENTENCE_SPOUT_ID, spout, 1);
        // SentenceSpout --> MyBolt
        builder.setBolt(BOLT_ID, bolt, 2).shuffleGrouping(SENTENCE_SPOUT_ID);

        Map config = new HashMap();
        if (args.length == 0) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
            waitForSeconds(120);
            cluster.killTopology(TOPOLOGY_NAME);
            cluster.shutdown();
            System.exit(0);
        } else if (args.length == 1) {
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        } else {
            System.out.println("Usage: S3Topology [topology name]");
        }
    }

    public static void waitForSeconds(int seconds) {
        try {
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException e) {
        }
    }

}
