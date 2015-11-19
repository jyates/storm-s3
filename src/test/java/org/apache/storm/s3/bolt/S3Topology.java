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
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import java.util.Map;
import java.util.UUID;

/**
 * A simple test topology for writing to S3. You can run the topology through {@link #main
 * (String[])} or as part of the test suite.
 */
@Category(S3DependentTests.class)
public class S3Topology extends BaseS3Topology {

    /**
     * Spin up a small storm cluster that writes 3 small files to s3 before stopping.
     */
    @Test
    public void testCluster() throws Exception {
        String bucket = randomBucket();
        BaseS3Topology.Cluster cluster = getCluster(BaseS3Topology.TOPOLOGY_NAME, bucket);
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
            BaseS3Topology.Cluster cluster = getCluster(
                  BaseS3Topology.TOPOLOGY_NAME, BaseS3Topology.BUCKET_NAME);
            cluster.start();
            cluster.stopAfter(BaseS3Topology.BUCKET_NAME, 3);
            System.exit(0);
        } else if (args.length == 1) {
            StormSubmitter
                  .submitTopology(args[0], config, getTopology(args[1], BaseS3Topology.BUCKET_NAME)
                        .createTopology());
        } else {
            System.out.println("Usage: S3Topology [topology name]");
        }
    }

    private static BaseS3Topology.Cluster getCluster(String topology, String bucketName) {
        BaseS3Topology.Cluster ret = new BaseS3Topology.Cluster();
        ret.setBuilder(getTopology(topology, bucketName));
        ret.setTopologyName(topology);
        return ret;
    }

    private static TopologyBuilder getTopology(String topologyName, String bucketName) {
        S3Bolt bolt = getBasicBolt(bucketName);
        // setup the uploader and a spy to allow us to stop after a known number of files
        Uploader uploader = new BlockingTransferManagerUploader();
        uploader.setEphemeralBucketForTesting(true);
        SpyingUploader spy = new SpyingUploader();
        spy.withDelegate(uploader);
        spy.withNameSpace(topologyName);
        bolt.setUploader(spy);

        return buildTopology(bolt);
    }
}
