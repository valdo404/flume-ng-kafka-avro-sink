/*******************************************************************************
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *******************************************************************************/
package org.redoop.flume.sink.avro.kafka;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.lang.reflect.Field;

import com.linkedin.camus.schemaregistry.MemorySchemaRegistry;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import org.apache.flume.*;
import org.apache.flume.Sink.Status;
import org.apache.flume.sink.AbstractSink;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.redoop.flume.sink.avro.kafka.KafkaAvroSink;

public class KafkaAvroSinkTest {

	private KafkaAvroSink mockKafkaSink;
	private Producer<byte[], byte[]> mockProducer;
	private Channel mockChannel;
	private Event mockEvent;
	private Transaction mockTx;

	@SuppressWarnings("unchecked")
	@Before
	public void setup() throws Exception {
		mockProducer = mock(Producer.class);
		mockChannel = mock(Channel.class);
		mockEvent = mock(Event.class);
		mockTx = mock(Transaction.class);
		mockKafkaSink = new KafkaAvroSink();
		Context context = new Context();
		context.put("topic", "test");
		context.put("avro.schema.file", getClass().getClassLoader().getResource("test.avsc").getFile());
		context.put("metadata.broker.list", "127.0.0.1:9092");
		context.put("parser.class", org.redoop.flume.sink.avro.kafka.parsers.HelloWorldParser.class.getName());
		context.put("kafka.message.coder.schema.registry.class", com.linkedin.camus.schemaregistry.FileSchemaRegistry.class.getName() );
		mockKafkaSink.configure(context);
		
		Field field = AbstractSink.class.getDeclaredField("channel");
		field.setAccessible(true);
		field.set(mockKafkaSink, mockChannel);

		field = KafkaAvroSink.class.getDeclaredField("topic");
		field.setAccessible(true);
		field.set(mockKafkaSink, "test");

		field = KafkaAvroSink.class.getDeclaredField("producer");
		field.setAccessible(true);
		field.set(mockKafkaSink, mockProducer);
		
		when(mockChannel.take()).thenReturn(mockEvent);
		when(mockChannel.getTransaction()).thenReturn(mockTx);
	}

	@After
	public void tearDown() throws Exception {
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStatusReady() throws EventDeliveryException, SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
		when(mockEvent.getBody()).thenReturn("frank".getBytes());
		Status status = mockKafkaSink.process();
		verify(mockChannel, times(1)).getTransaction();
		verify(mockChannel, times(1)).take();
		verify(mockProducer, times(1)).send((KeyedMessage<byte[], byte[]>) any());
		verify(mockTx, times(1)).commit();
		verify(mockTx, times(0)).rollback();
		verify(mockTx, times(1)).close();
		assertEquals(Status.READY, status);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStatusBackoff() throws EventDeliveryException, SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
		when(mockEvent.getBody()).thenThrow(new RuntimeException());
		Status status = mockKafkaSink.process();
		verify(mockChannel, times(1)).getTransaction();
		verify(mockChannel, times(1)).take();
		verify(mockProducer, times(0)).send((KeyedMessage<byte[], byte[]>) any());
		verify(mockTx, times(0)).commit();
		verify(mockTx, times(1)).rollback();
		verify(mockTx, times(1)).close();
		assertEquals(Status.BACKOFF, status);
	}
}
