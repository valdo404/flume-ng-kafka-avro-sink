package org.redoop.flume.sink.avro.kafka;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import com.linkedin.camus.coders.MessageDecoderException;
import com.linkedin.camus.schemaregistry.CachedSchemaRegistry;
import com.linkedin.camus.schemaregistry.SchemaRegistry;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;

/**
 * Powerspace code created by lvaldes on 23/12/15. All rights reserved.
 */
public class DelegatingKafkaAvroMessageDecoder extends MessageDecoder<byte[], GenericData.Record> {
    protected DecoderFactory decoderFactory;
    protected SchemaRegistry<Schema> registry;
    private Schema latestSchema;

    @SuppressWarnings("unchecked")
    public DelegatingKafkaAvroMessageDecoder(String topicName, Configuration conf) {
        this.topicName = topicName;

    }


    public void init(Properties props, String topicName, SchemaRegistry<Schema> registry) {
        super.init(props, topicName);

        try {
            registry.init(props);

            this.registry = new CachedSchemaRegistry<>(registry);
            this.latestSchema = registry.getLatestSchemaByTopic(topicName).getSchema();
        } catch (Exception e) {
            throw new MessageDecoderException(e);
        }

        decoderFactory = DecoderFactory.get();
    }

    @Override
    public void init(Properties props, String topicName) {
        throw new NotImplementedException("Please call with registry");
    }

    public class MessageDecoderHelper {
        //private Message message;
        private ByteBuffer buffer;
        private Schema schema;
        private int start;
        private int length;
        private Schema targetSchema;
        private static final byte MAGIC_BYTE = 0x0;
        private final SchemaRegistry<Schema> registry;
        private final String topicName;
        private byte[] payload;

        public MessageDecoderHelper(SchemaRegistry<Schema> registry, String topicName, byte[] payload) {
            this.registry = registry;
            this.topicName = topicName;
            //this.message = message;
            this.payload = payload;
        }

        public ByteBuffer getBuffer() {
            return buffer;
        }

        public Schema getSchema() {
            return schema;
        }

        public int getStart() {
            return start;
        }

        public int getLength() {
            return length;
        }

        public Schema getTargetSchema() {
            return targetSchema;
        }

        private ByteBuffer getByteBuffer(byte[] payload) {
            ByteBuffer buffer = ByteBuffer.wrap(payload);
            if (buffer.get() != MAGIC_BYTE)
                throw new IllegalArgumentException("Unknown magic byte!");
            return buffer;
        }

        public MessageDecoderHelper invoke() {
            buffer = getByteBuffer(payload);
            String id = Integer.toString(buffer.getInt());
            schema = registry.getSchemaByID(topicName, id);
            if (schema == null)
                throw new IllegalStateException("Unknown schema id: " + id);

            start = buffer.position() + buffer.arrayOffset();
            length = buffer.limit() - 5;

            // try to get a target schema, if any
            targetSchema = latestSchema;
            return this;
        }
    }

    public CamusWrapper<GenericData.Record> decode(byte[] payload) {
        try {
            MessageDecoderHelper helper = new MessageDecoderHelper(registry, topicName, payload).invoke();
            DatumReader<GenericData.Record> reader =
                    (helper.getTargetSchema() == null) ? new GenericDatumReader<GenericData.Record>(helper.getSchema())
                            : new GenericDatumReader<GenericData.Record>(helper.getSchema(), helper.getTargetSchema());

            return new CamusAvroWrapper(reader.read(null,
                    decoderFactory.binaryDecoder(helper.getBuffer().array(), helper.getStart(), helper.getLength(), null)));

        } catch (IOException e) {
            throw new MessageDecoderException(e);
        }
    }

    public static class CamusAvroWrapper extends CamusWrapper<GenericData.Record> {
        public CamusAvroWrapper(GenericData.Record record) {
            super(record);
            GenericData.Record header = (GenericData.Record) super.getRecord().get("header");
            if (header != null) {
                if (header.get("server") != null) {
                    put(new Text("server"), new Text(header.get("server").toString()));
                }
                if (header.get("service") != null) {
                    put(new Text("service"), new Text(header.get("service").toString()));
                }
            }
        }

        @Override
        public long getTimestamp() {
            GenericData.Record header = (GenericData.Record) super.getRecord().get("header");

            if (header != null && header.get("time") != null) {
                return (Long) header.get("time");
            } else if (super.getRecord().get("timestamp") != null) {
                return (Long) super.getRecord().get("timestamp");
            } else {
                return System.currentTimeMillis();
            }
        }
    }
}
