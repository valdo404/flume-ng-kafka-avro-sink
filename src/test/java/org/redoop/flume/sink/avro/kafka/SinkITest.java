package org.redoop.flume.sink.avro.kafka;

import kafka.admin.TopicCommand;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.*;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.flume.*;
import org.apache.flume.sink.AbstractSink;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.verify;

/**
 * Powerspace code created by lvaldes on 24/12/15. All rights reserved.
 */
public class SinkITest {
    @Test
    public void testConsumption() throws NoSuchFieldException, IllegalAccessException, EventDeliveryException, IOException {
        //given I configure a kafka broker
        int brokerId = 1;
        String topic = "aaaaa";

        EmbeddedZookeeper zkServer = new EmbeddedZookeeper(TestZKUtils.zookeeperConnect());
        String zkConnect = zkServer.connectString();
        ZkClient zkClient = zookeeperClient(zkConnect);
        int port = TestUtils.choosePort();

        Properties prop = TestUtils.createBrokerConfig(brokerId, port, true);
        KafkaServer kafkaServer = initializeKafka(brokerId, port, topic, zkClient);

        //given I configure producers and consumers
        Producer producer = buildProducer(port);
        ConsumerConnector consumer = buildConsumer(zkConnect);

        //given I define a sink
        RawKafkaAvroSink mockKafkaSink = new RawKafkaAvroSink();

        //given I define channels and transactions
        Channel mockChannel = mock(Channel.class);
        Event event = mock(Event.class);
        Transaction transaction = mock(Transaction.class);

        when(mockChannel.take()).thenReturn(event);
        when(mockChannel.getTransaction()).thenReturn(transaction);

        mockFields(topic, producer, mockKafkaSink, mockChannel);

        //when I define an avro schema
        String schemaFile = getClass().getClassLoader().getResource("test.avsc").getFile();
        when(event.getBody()).thenReturn(getSampleBytes(schemaFile));

        //when I configure the sink
        Context context = new Context();
        context.put("topic", topic);
        context.put("avro.schema.file", schemaFile);
        context.put("metadata.broker.list", prop.get("host.name").toString() + ":" + prop.get("port").toString());
        context.put("parser.class", org.redoop.flume.sink.avro.kafka.parsers.HelloWorldParser.class.getName());
        context.put("kafka.message.coder.schema.registry.class", com.linkedin.camus.schemaregistry.MemorySchemaRegistry.class.getName());

        mockKafkaSink.configure(context);

        zkClient.delete("/consumers/group0");

        //then everything goes right when I launch process
        Sink.Status status = mockKafkaSink.process();
        assertEquals(status, Sink.Status.READY);

        //and message can be retrieved in kafka
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicMap(topic));

        KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();

        if(iterator.hasNext()) {
            String msg = new String(iterator.next().message(), StandardCharsets.UTF_8);
            System.out.println(msg);
        } else {
            fail();
        }

        // and finally the cluster can be shutdown without errors
        consumer.shutdown();
        kafkaServer.shutdown();
        zkClient.close();
        zkServer.shutdown();
    }

    private void mockFields(String topic, Producer producer, RawKafkaAvroSink mockKafkaSink, Channel mockChannel) throws NoSuchFieldException, IllegalAccessException {
        Field field = AbstractSink.class.getDeclaredField("channel");
        field.setAccessible(true);
        field.set(mockKafkaSink, mockChannel);

        field = KafkaAvroSink.class.getDeclaredField("topic");
        field.setAccessible(true);
        field.set(mockKafkaSink, topic);

        field = KafkaAvroSink.class.getDeclaredField("producer");
        field.setAccessible(true);
        field.set(mockKafkaSink, producer);
    }

    private byte[] getSampleBytes(String schemaFile) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        EncoderFactory encoderFactory = EncoderFactory.get();
        BinaryEncoder encoder = encoderFactory.directBinaryEncoder(out, null);
        DatumWriter<IndexedRecord> writer;

        Schema schema = KafkaAvroSinkUtil.schemaFromFile(new File(schemaFile));
        GenericRecord record = new GenericData.Record(schema);

        record.put("Action", "patate");

        writer = new GenericDatumWriter<>(record.getSchema());
        writer.write(record, encoder);

        return out.toByteArray();
    }

    private Map<String, Integer> topicMap(String topic) {
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, 1);
        return topicCountMap;
    }

    private ZkClient zookeeperClient(String zkConnect) {
        return new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer$.MODULE$);
    }

    private Producer buildProducer(int port) {
        return new Producer(
                new ProducerConfig(TestUtils.getProducerConfig("localhost:" + port)));
    }

    private ConsumerConnector buildConsumer(String zkConnect) {
        return kafka.consumer.Consumer.createJavaConsumerConnector(
                new ConsumerConfig(TestUtils.createConsumerProperties(zkConnect, "group0", "consumer0", -1)));
    }

    private KafkaServer initializeKafka(int brokerId, int port, String topic, ZkClient zkClient) {
        // kafka server
        KafkaServer kafkaServer = TestUtils.createServer(
                new KafkaConfig(TestUtils.createBrokerConfig(brokerId, port, true)), new MockTime());
        // create topic
        TopicCommand.createTopic(zkClient,
                new TopicCommand.TopicCommandOptions(new String[]{"--topic", topic, "--partitions", "1","--replication-factor", "1"}));

        List<KafkaServer> servers = Collections.singletonList(kafkaServer);
        TestUtils.waitUntilMetadataIsPropagated(scala.collection.JavaConversions.asScalaBuffer(servers), topic, 0, 60000);
        return kafkaServer;
    }
}
