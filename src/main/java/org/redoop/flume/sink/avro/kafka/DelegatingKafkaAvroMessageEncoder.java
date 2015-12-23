package org.redoop.flume.sink.avro.kafka;

import com.linkedin.camus.coders.MessageEncoder;
import com.linkedin.camus.coders.MessageEncoderException;
import com.linkedin.camus.etl.kafka.coders.KafkaAvroMessageEncoder;
import com.linkedin.camus.schemaregistry.SchemaRegistry;
import com.linkedin.camus.schemaregistry.SchemaRegistryException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class DelegatingKafkaAvroMessageEncoder extends MessageEncoder<IndexedRecord, byte[]> {
    private static final byte MAGIC_BYTE = 0x0;
    private static final Logger logger = Logger.getLogger(KafkaAvroMessageEncoder.class);

    private SchemaRegistry<Schema> client;
    private final Map<Schema, String> cache = Collections.synchronizedMap(new HashMap<Schema, String>());
    private final EncoderFactory encoderFactory = EncoderFactory.get();


    @SuppressWarnings("unchecked")
    public DelegatingKafkaAvroMessageEncoder(String topicName, Configuration conf) {
        this.topicName = topicName;
    }

    public void init(Properties props, String topicName,SchemaRegistry<Schema> registry) {
        this.props = props;
        this.topicName = topicName;

        try {
            client = registry;
            client.init(props);
        } catch (Exception e) {
            throw new MessageEncoderException(e);
        }

    }

    public void init(Properties props, String topicName) {
        throw new NotImplementedException("Please call with registry");
    }

    public byte[] toBytes(IndexedRecord record) {
        try {

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.write(MAGIC_BYTE);

            Schema schema = record.getSchema();

            // auto-register schema if it is not in the cache
            String id;
            if (!cache.containsKey(schema)) {
                try {
                    id = client.register(topicName, record.getSchema());
                    cache.put(schema, id);
                } catch (SchemaRegistryException e) {
                    throw new RuntimeException(e);
                }
            } else {
                id = cache.get(schema);
            }

            out.write(ByteBuffer.allocate(4).putInt(Integer.parseInt(id)).array());

            BinaryEncoder encoder = encoderFactory.directBinaryEncoder(out, null);
            DatumWriter<IndexedRecord> writer;

            if (record instanceof SpecificRecord)
                writer = new SpecificDatumWriter<>(record.getSchema());
            else
                writer = new GenericDatumWriter<>(record.getSchema());
            writer.write(record, encoder);

            return out.toByteArray();
        } catch (IOException e) {
            throw new MessageEncoderException(e);
        }
    }

}
