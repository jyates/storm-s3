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

import org.apache.storm.s3.upload.NoOpUploader;
import org.apache.storm.s3.upload.SpyingUploader;

import org.junit.Test;

import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import java.util.HashMap;
import java.util.Map;

/**
 * Start a test topology but use an {@link SpyingUploader} to do the uploads.
 */
public class LocalS3TopologyTest extends BaseS3Topology {

    /**
     * Runs a simple topology that just writes sentences continuously until it is stopped after a
     * handful of files
     *
     * @throws Exception on failure
     */
    @Test
    public void testSimpleTopology() throws Exception {
        String bucket = randomBucket();
        // basic bolt setup
        S3Bolt bolt = getBasicBolt(bucket);

        // use the in memory uploader
        SpyingUploader uploader = new SpyingUploader();
        uploader.withNameSpace(TOPOLOGY_NAME);
        uploader.withDelegate(new NoOpUploader());
        bolt.setUploader(uploader);

        TopologyBuilder builder = buildTopology(bolt);
        Cluster cluster = new Cluster();
        cluster.setBuilder(builder);
        cluster.start();
        cluster.stopAfter(bucket, 3);
    }
}