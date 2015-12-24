package org.redoop.flume.sink.avro.kafka;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.flume.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;

/**
 * Powerspace code created by lvaldes on 24/12/15. All rights reserved.
 */
public class RawKafkaAvroSink extends KafkaAvroSink {
    private static final Logger log = LoggerFactory.getLogger(KafkaAvroSink.class);

    @Override
    public void configure(Context context) {
        super.configure(context);
    }

    protected IndexedRecord buildRecord(byte[] body) throws IOException {
        DecoderFactory decoderFactory = DecoderFactory.get();
        DatumReader<Record> reader = new GenericDatumReader<>(schema, decoder.getLatestSchema());

        return reader.read(null, decoderFactory.directBinaryDecoder(new ByteArrayInputStream(body), null));
    }
}
