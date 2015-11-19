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
package org.apache.storm.s3.bolt;

import org.apache.storm.s3.S3DependentTests;
import org.apache.storm.s3.format.AbstractFileNameFormat;
import org.apache.storm.s3.format.DefaultFileNameFormat;
import org.apache.storm.s3.format.DelimitedRecordFormat;
import org.apache.storm.s3.format.RecordFormat;
import org.apache.storm.s3.format.S3OutputConfiguration;
import org.apache.storm.s3.output.upload.BlockingTransferManagerUploader;
import org.apache.storm.s3.output.upload.Uploader;
import org.apache.storm.s3.rotation.FileRotationPolicy;
import org.apache.storm.s3.rotation.FileSizeRotationPolicy;
import org.apache.storm.s3.upload.SpyingUploader;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import java.util.Map;
import java.util.UUID;

/**
 * A simple test topology for writing to S3. You can run the topology through {@link #main
 * (String[])} or as part of the test suite.
 */
@Category(S3DependentTests.class)
public class S3Topology {
    static final String SENTENCE_SPOUT_ID = "sentence-spout";
    static final String BOLT_ID = "my-bolt";
    static final String TOPOLOGY_NAME = "test-topology";
    static final String BUCKET_NAME = "test-bucket";

    /**
     * Spin up a small storm cluster that writes 3 small files to s3 before stopping.
     *
     * @throws Exception
     */
    @Test
    public void testCluster() throws Exception {
        String bucket = BUCKET_NAME + "-" + UUID.randomUUID();
        Cluster cluster = getCluster(TOPOLOGY_NAME, bucket);
        cluster.start();
        cluster.stopAfter(bucket, 3);
    }

    /**
     * Run the cluster. If you supply a configuration you can submit a topology to a real cluster
     * . However, the {@link SpyingUploader} blocking ability is not supported in non-JVM-local
     * clusters
     *
     * @param args name of the topology
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Map config = new Config();
        if (args.length == 0) {
            Cluster cluster = getCluster(TOPOLOGY_NAME, BUCKET_NAME);
            cluster.start();
            cluster.stopAfter(BUCKET_NAME, 3);
            System.exit(0);
        } else if (args.length == 1) {
            StormSubmitter.submitTopology(args[0], config, getTopology(args[1], BUCKET_NAME)
                  .createTopology());
        } else {
            System.out.println("Usage: S3Topology [topology name]");
        }
    }

    private static Cluster getCluster(String topology, String bucketName) {
        Cluster ret = new Cluster();
        ret.setBuilder(getTopology(topology, bucketName));
        ret.setTopologyName(topology);
        return ret;
    }

    private static TopologyBuilder getTopology(String topologyName, String bucketName) {
        // setup the bolt
        S3Bolt bolt = new S3Bolt();
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(1.0F,
              FileSizeRotationPolicy.Units.KB);
        bolt.setRotationPolicy(rotationPolicy);
        AbstractFileNameFormat format = new DefaultFileNameFormat().withExtension(".txt")
                                                                   .withPrefix("test");
        bolt.setFileNameFormat(format);
        RecordFormat recordFormat = new DelimitedRecordFormat();
        bolt.setRecordFormat(recordFormat);

        // setup the uploader and a spy to allow us to stop after a known number of files
        Uploader uploader = new BlockingTransferManagerUploader();
        uploader.setEphemeralBucketForTesting(true);
        SpyingUploader spy = new SpyingUploader();
        spy.withDelegate(uploader);
        spy.withNameSpace(topologyName);
        bolt.setUploader(spy);

        S3OutputConfiguration s3 =
              new S3OutputConfiguration().setBucket(bucketName)
                                         .setContentType("text/plain")
                                         .withPath("foo");
        bolt.setS3Location(s3);

        TopologyBuilder builder = new TopologyBuilder();

        SentenceSpout spout = new SentenceSpout();
        builder.setSpout(SENTENCE_SPOUT_ID, spout, 1);
        // SentenceSpout --> MyBolt
        builder.setBolt(BOLT_ID, bolt, 1).shuffleGrouping(SENTENCE_SPOUT_ID);
        return builder;
    }

    private static class Cluster {
        LocalCluster cluster;
        String topologyName;
        private Map config = new Config();
        private TopologyBuilder builder;

        public void setConfig(Map config) {
            this.config = config;
        }

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
