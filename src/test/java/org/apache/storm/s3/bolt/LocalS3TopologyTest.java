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
import org.apache.storm.s3.rotation.FileRotationPolicy;
import org.apache.storm.s3.rotation.FileSizeRotationPolicy;
import org.apache.storm.s3.upload.NoOpUploader;
import org.apache.storm.s3.upload.SpyingUploader;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Start a test topology but use an {@link SpyingUploader} to do the uploads.
 */
public class LocalS3TopologyTest {

    @Rule
    public TestName name = new TestName();

    /**
     * Runs a simple topology that just writes sentences continuously until it is stopped after a
     * handful of files
     *
     * @throws Exception on failure
     */
    @Test
    public void testSimpleTopology() throws Exception {
        String spoutId = "sentences", boltId = "uploader", topology = name.getMethodName();

        // basic bolt setup
        S3Bolt bolt = new S3Bolt();
        addFormat(bolt);
        addRecordFormat(bolt);
        String bucketName = "test-bucket";
        addOutput(bolt, bucketName);

        // turn down the rotation size so we can get a couple of files and not blow up memory
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(1.0F,
              FileSizeRotationPolicy.Units.KB);
        bolt.setRotationPolicy(rotationPolicy);

        // use the in memory uploader
        SpyingUploader uploader = new SpyingUploader();
        uploader.setNameSpace(topology);
        uploader.withDelegate(new NoOpUploader());
        bolt.setUploader(uploader);

        // build the topology
        TopologyBuilder builder = new TopologyBuilder();
        SentenceSpout spout = new SentenceSpout();

        builder.setSpout(spoutId, spout, 1);
        builder.setBolt(boltId, bolt, 1).shuffleGrouping(spoutId);

        // submit the topology
        Map config = new HashMap();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(topology, config, builder.createTopology());

        // wait for some data to be accrued
        SpyingUploader.waitForFileCount(topology, bucketName, 3);
        cluster.killTopology(topology);
        cluster.shutdown();
    }

    private void addOutput(S3Bolt bolt, String bucketName) {
        S3OutputConfiguration s3 =
              new S3OutputConfiguration().setBucket(bucketName)
                                         .setContentType("text/plain")
                                         .withPath("foo");
        bolt.setS3Location(s3);
    }

    private void addRecordFormat(S3Bolt bolt) {
        RecordFormat recordFormat = new DelimitedRecordFormat();
        bolt.setRecordFormat(recordFormat);
    }

    private void addFormat(S3Bolt bolt) {
        AbstractFileNameFormat format = new DefaultFileNameFormat().withExtension(".txt")
                                                                   .withPrefix("test");
        bolt.setFileNameFormat(format);
    }
}