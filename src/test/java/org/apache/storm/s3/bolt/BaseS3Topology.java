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

import org.apache.storm.s3.format.AbstractFileNameFormat;
import org.apache.storm.s3.format.DefaultFileNameFormat;
import org.apache.storm.s3.format.DelimitedRecordFormat;
import org.apache.storm.s3.format.RecordFormat;
import org.apache.storm.s3.format.S3OutputConfiguration;
import org.apache.storm.s3.output.upload.Uploader;
import org.apache.storm.s3.rotation.FileRotationPolicy;
import org.apache.storm.s3.rotation.FileSizeRotationPolicy;
import org.apache.storm.s3.upload.SpyingUploader;

import org.junit.Rule;
import org.junit.rules.TestName;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import java.util.Map;
import java.util.UUID;

/**
 *
 */
public abstract class BaseS3Topology {

    static final String SENTENCE_SPOUT_ID = "sentence-spout";
    static final String BOLT_ID = "my-bolt";
    static final String TOPOLOGY_NAME = "test-topology";
    static final String BUCKET_NAME = "test-bucket";

    @Rule
    public TestName name = new TestName();

    String randomBucket() {
        return name.getMethodName() + "-" + UUID.randomUUID();
    }

    static TopologyBuilder buildTopology(S3Bolt bolt) {
        TopologyBuilder builder = new TopologyBuilder();

        SentenceSpout spout = new SentenceSpout();
        builder.setSpout(BaseS3Topology.SENTENCE_SPOUT_ID, spout, 1);
        // SentenceSpout --> MyBolt
        builder.setBolt(BaseS3Topology.BOLT_ID, bolt, 1).shuffleGrouping(
              BaseS3Topology.SENTENCE_SPOUT_ID);
        return builder;
    }


    static S3Bolt getBasicBolt(String bucketName) {
        // setup the bolt
        S3Bolt bolt = new S3Bolt();
        addFileNameFormat(bolt);
        addRecordFormat(bolt);
        addRotationPolicy(bolt);
        addS3Location(bolt, bucketName);
        addOutput(bolt, bucketName);
        return bolt;
    }

    static void addOutput(S3Bolt bolt, String bucketName) {
        S3OutputConfiguration s3 =
              new S3OutputConfiguration().setBucket(bucketName)
                                         .setContentType("text/plain")
                                         .withPath("foo");
        bolt.setS3Location(s3);
    }

    static void addRecordFormat(S3Bolt bolt) {
        RecordFormat recordFormat = new DelimitedRecordFormat();
        bolt.setRecordFormat(recordFormat);
    }

    static void addFileNameFormat(S3Bolt bolt) {
        AbstractFileNameFormat format = new DefaultFileNameFormat().withExtension(".txt")
                                                                   .withPrefix("test");
        bolt.setFileNameFormat(format);
    }

    static void addRotationPolicy(S3Bolt bolt) {
        // turn down the rotation size so we can get a couple of files and not blow up memory
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(1.0F,
              FileSizeRotationPolicy.Units.KB);
        bolt.setRotationPolicy(rotationPolicy);
    }

    static void addS3Location(S3Bolt bolt, String bucketName) {
        S3OutputConfiguration s3 =
              new S3OutputConfiguration().setBucket(bucketName)
                                         .setContentType("text/plain")
                                         .withPath("foo");
        bolt.setS3Location(s3);
    }

    static class Cluster {
        LocalCluster cluster;
        String topologyName = TOPOLOGY_NAME;
        private Map config = new Config();
        private TopologyBuilder builder;

        public void setBuilder(TopologyBuilder builder) {
            this.builder = builder;
        }

        public void start() {
            this.cluster = new LocalCluster();
            cluster.submitTopology(topologyName, config, builder.createTopology());
        }

        public void stopAfter(String bucket, int files) throws InterruptedException {
            SpyingUploader.waitForFileCount(topologyName, bucket, files);
            cluster.killTopology(topologyName);
            // stop writing any more files - we know it works and more just cost $$$
            SpyingUploader.block(topologyName);
            cluster.shutdown();
        }

        public void setTopologyName(String topology) {
            this.topologyName = topology;
        }
    }
}
