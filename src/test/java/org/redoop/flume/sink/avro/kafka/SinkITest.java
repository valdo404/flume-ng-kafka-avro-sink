package org.redoop.flume.sink.avro.kafka;

import kafka.admin.TopicCommand;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.*;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.apache.flume.*;
import org.apache.flume.sink.AbstractSink;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.*;
import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Powerspace code created by lvaldes on 24/12/15. All rights reserved.
 */
public class SinkITest {
    @Test
    public void testConsumption() throws NoSuchFieldException, IllegalAccessException, EventDeliveryException {
        int brokerId = 1;
        String topic = "aaaaa";

        EmbeddedZookeeper zkServer = new EmbeddedZookeeper(TestZKUtils.zookeeperConnect());
        String zkConnect = zkServer.connectString();
        ZkClient zkClient = zookeeperClient(zkConnect);
        int port = TestUtils.choosePort();

        KafkaServer kafkaServer = initializeKafka(brokerId, port, topic, zkClient);

        //producer / consumer
        Producer producer = buildProducer(port);
        ConsumerConnector consumer = buildConsumer(zkConnect);

        // send message
        KafkaAvroSink mockKafkaSink = new KafkaAvroSink();

        Channel mockChannel = mock(Channel.class);
        Event event = mock(Event.class);
        Transaction transaction = mock(Transaction.class);

        Properties prop = TestUtils.createBrokerConfig(brokerId, TestUtils.choosePort(), true);

        Context context = new Context();
        context.put("topic", topic);
        context.put("avro.schema.file", getClass().getClassLoader().getResource("test.avsc").getFile());
        context.put("metadata.broker.list", prop.get("host.name").toString() + ":" + prop.get("port").toString());
        context.put("parser.class", org.redoop.flume.sink.avro.kafka.parsers.HelloWorldParser.class.getName());
        context.put("kafka.message.coder.schema.registry.class", com.linkedin.camus.schemaregistry.MemorySchemaRegistry.class.getName());

        mockKafkaSink.configure(context);

        Field field = AbstractSink.class.getDeclaredField("channel");
        field.setAccessible(true);
        field.set(mockKafkaSink, mockChannel);

        when(mockChannel.take()).thenReturn(event);
        when(mockChannel.getTransaction()).thenReturn(transaction);
        when(event.getBody()).thenReturn("frank".getBytes());

        field = KafkaAvroSink.class.getDeclaredField("topic");
        field.setAccessible(true);
        field.set(mockKafkaSink, topic);

        field = KafkaAvroSink.class.getDeclaredField("producer");
        field.setAccessible(true);
        field.set(mockKafkaSink, producer);

        zkClient.delete("/consumers/group0");

        Sink.Status status = mockKafkaSink.process();
        assertEquals(status, Sink.Status.READY);

        /* consume */
        // deleting zookeeper information to make sure the consumer starts from the beginning
        // starting consumer

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicMap(topic));

        KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();

        if(iterator.hasNext()) {
            String msg = new String(iterator.next().message(), StandardCharsets.UTF_8);
            System.out.println(msg);
        } else {
            fail();
        }

        // cleanup
        consumer.shutdown();
        kafkaServer.shutdown();
        zkClient.close();
        zkServer.shutdown();
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
