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

/**
 * Test that the manager acks tuples as soon as they are received, regardless of the commit state
 */
public class ImmediateAckManagerTest {

    @Test
    public void testAcks() throws IOException {
        OutputCollector collector = Mockito.mock(OutputCollector.class);
        ImmediateAckManager manager = new ImmediateAckManager();
        manager.prepare(collector);

        // ack when future is complete
        Tuple t = Mockito.mock(Tuple.class);
        manager.handleAck(t, Futures.immediateFuture(null));
        Mockito.verify(collector).ack(t);

        // ack when future is not complete
        SettableFuture<Void> future = SettableFuture.create();
        manager.handleAck(t,future);
        Mockito.verify(collector, Mockito.times(2)).ack(t);

        // ack when nothing committed
        manager.handleAck(t,null);
        Mockito.verify(collector, Mockito.times(3)).ack(t);

        manager.fail(t);
        Mockito.verify(collector).fail(t);
    }
}
