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

import org.apache.storm.guava.util.concurrent.Futures;
import org.apache.storm.guava.util.concurrent.SettableFuture;

import org.junit.Test;
import org.mockito.Mockito;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Test that the concurrent ack manager correctly manages a single and multiple concurrent uploads
 */
public class MultiConcurrentAckManagerTest {

    @Test
    public void testSingleAckManager() throws Exception{
        OutputCollector collector = Mockito.mock(OutputCollector.class);
        MultiConcurrentUploadAckManager manager = new MultiConcurrentUploadAckManager();
        manager.setConcurrentUploads(1);

        manager.prepare(collector);
        Tuple t = Mockito.mock(Tuple.class);
        manager.handleAck(t, null);
        manager.handleAck(t, null);
        manager.handleAck(t, Futures.<Void>immediateFuture(null));

        Mockito.verify(collector, Mockito.times(3)).ack(t);
    }

    @Test
    public void testMultipleUploads() throws Exception{
        OutputCollector collector = Mockito.mock(OutputCollector.class);
        MultiConcurrentUploadAckManager manager = new MultiConcurrentUploadAckManager();

        manager.prepare(collector);
        Tuple t = Mockito.mock(Tuple.class);
        manager.handleAck(t, null);
        manager.handleAck(t, null);
        SettableFuture<Void> future = SettableFuture.create();
        manager.handleAck(t, future);

        // at this point, we are looking at the second ack manager
        Tuple t2 = Mockito.mock(Tuple.class);
        manager.handleAck(t2, null);
        manager.handleAck(t2, null);
        SettableFuture<Void> future2 = SettableFuture.create();
        manager.handleAck(t2, future);

        // there are no more managers on the queue, so we have to wait for a new manager
        assertEquals(0, manager.getManagersForTesting().size());
        Tuple t3 = Mockito.mock(Tuple.class);
        AtomicBoolean added = new AtomicBoolean(false);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(1);
        Thread thread = new Thread(() ->{
            try {
                started.countDown();
                manager.handleAck(t3, null);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }finally {
                added.set(true);
                completed.countDown();
            }
        });
        thread.start();
        //make sure the thread starts and then wait a little bit longer to make sure it isn't
        // added yet
        started.await();
        Thread.sleep(500);
        assertFalse(added.get());

        // complete the first future
        future.set(null);
        // wait for the manager to be added back to the queue, at which point all its tuples must
        // have been acked
        completed.await();
        Mockito.verify(collector, Mockito.times(3)).ack(t);
    }
}
