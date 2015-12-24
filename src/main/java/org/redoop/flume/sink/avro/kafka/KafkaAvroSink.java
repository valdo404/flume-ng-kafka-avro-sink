/*******************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *******************************************************************************/
package org.redoop.flume.sink.avro.kafka;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Properties;

import com.linkedin.camus.etl.kafka.coders.KafkaAvroMessageEncoder;
import com.linkedin.camus.schemaregistry.SchemaRegistry;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.sink.AbstractSink;
import org.redoop.flume.sink.avro.kafka.parsers.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Sink of Kafka which get events from channels and publish to Kafka Serialized in Avro.
 * <tt>zk.connect: </tt> the zookeeper ip kafka use.
 * <p>
 * <tt>topic: </tt> the topic to read from kafka.
 * <p>
 * <tt>batchSize: </tt> send serveral messages in one request to kafka.
 * <p>
 * <tt>producer.type: </tt> type of producer of kafka, async or sync is
 * available.<o> <tt>serializer.class: </tt>{@kafka.serializer.StringEncoder
 *
 *
 * }
 */
public abstract class KafkaAvroSink extends AbstractSink implements Configurable {
    private static final Logger log = LoggerFactory.getLogger(KafkaAvroSink.class);

    protected String topic;
    protected Producer<byte[], byte[]> producer;
    protected Schema schema;

    protected Properties props;

    protected DelegatingKafkaAvroMessageEncoder encoder;
    protected DelegatingKafkaAvroMessageDecoder decoder;

    public Status process() throws EventDeliveryException {
        Channel channel = getChannel();
        Transaction tx = channel.getTransaction();
        try {
            tx.begin();
            Event event = channel.take();
            if (event == null) {
                tx.commit();
                return Status.READY;
            }

            IndexedRecord record = buildRecord(event.getBody());

            byte[] avroRecord = encoder.toBytes(record);

            producer.send(new KeyedMessage<byte[], byte[]>(this.topic, avroRecord));

            tx.commit();
            return Status.READY;
        } catch (Exception e) {
            try {
                tx.rollback();
                return Status.BACKOFF;
            } catch (Exception e2) {
                log.error("Rollback Exception:{}", e2);
            }
            log.error("Avro sink exception:{}", e);
            return Status.BACKOFF;
        } finally {
            tx.close();
        }
    }

    protected abstract IndexedRecord buildRecord(byte[] body) throws IOException;

    @Override
    public void configure(Context context) {
        topic = context.getString("topic");
        if (topic == null) {
            throw new ConfigurationException("Kafka topic must be specified.");
        }
        // Get schema file
        String avroSchemaFileName = context.getString("avro.schema.file");

        File avroSchemaFile;
        if (avroSchemaFileName == null) {
            throw new ConfigurationException("Avro schema must be specified.");
        } else {
            avroSchemaFile = new File(avroSchemaFileName);
        }

        producer = KafkaAvroSinkUtil.getProducer(context);
        props = KafkaAvroSinkUtil.getKafkaConfigProperties(context);

        encoder = new DelegatingKafkaAvroMessageEncoder(topic, null);

        SchemaRegistry<Schema> registry = null;
        try {
            registry = (SchemaRegistry<Schema>) Class.forName(props.getProperty(KafkaAvroMessageEncoder.KAFKA_MESSAGE_CODER_SCHEMA_REGISTRY_CLASS)).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            log.error("Cannot instanciate registry", e);
            stop();
            return;
        }

        try {
            schema = KafkaAvroSinkUtil.schemaFromFile(avroSchemaFile);
            registry.register(topic, KafkaAvroSinkUtil.schemaFromFile(avroSchemaFile));
        } catch (IOException e) {
            log.error("Cannot find file", e);
            stop();
            return;
        }

        encoder.init(props, topic, registry);

        decoder = new DelegatingKafkaAvroMessageDecoder(topic, null);
        decoder.init(props, topic, registry);
    }



    @Override
    public synchronized void start() {
        super.start();
    }

    @Override
    public synchronized void stop() {
        producer.close();
        super.stop();
    }
}
