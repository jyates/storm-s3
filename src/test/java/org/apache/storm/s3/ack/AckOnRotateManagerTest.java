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

import org.apache.storm.guava.util.concurrent.AbstractFuture;
import org.apache.storm.guava.util.concurrent.Futures;

import org.junit.Test;
import org.mockito.Mockito;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

/**
 *
 */
public class AckOnRotateManagerTest {

    @Test
    public void testAckOnRotate() throws Exception{
        OutputCollector collector = Mockito.mock(OutputCollector.class);
        AckOnRotateManager manager = new AckOnRotateManager();
        manager.prepare(collector);

        // no ack when nothing committed yet
        Tuple t = Mockito.mock(Tuple.class);
        manager.handleAck(t, null);
        Mockito.verifyZeroInteractions(collector);

        // ack when future is complete
        manager.handleAck(t, Futures.immediateFuture(null));
        Mockito.verify(collector, Mockito.times(2)).ack(t);
    }

    @Test
    public void testAckOnCommitCompletion() throws Exception{
        OutputCollector collector = Mockito.mock(OutputCollector.class);
        AckOnRotateManager manager = new AckOnRotateManager();
        manager.prepare(collector);

        // no ack when nothing committed yet
        Tuple t = Mockito.mock(Tuple.class);
        manager.handleAck(t, null);
        Mockito.verifyZeroInteractions(collector);

        // ack when future is complete
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch setValue = new CountDownLatch(1);
        AbstractFuture<Void> future = new AbstractFuture<Void>() {
            @Override
            public Void get() throws InterruptedException, ExecutionException {
                started.countDown();
                setValue.await();
                this.set(null);
                return super.get();
            }
        };
        CountDownLatch finished = new CountDownLatch(1);
        Thread thread = new Thread(() -> {
            try {
                manager.handleAck(t, future);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }finally {
                finished.countDown();
            }
        });
        thread.start();
        started.await();
        // make sure we haven't acked anything yet
        Mockito.verifyZeroInteractions(collector);

        // finish the commit
        setValue.countDown();
        finished.await();
        Mockito.verify(collector, Mockito.times(2)).ack(t);
    }
}
