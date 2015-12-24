package org.redoop.flume.sink.avro.kafka;

import org.apache.avro.generic.IndexedRecord;
import org.apache.flume.Context;
import org.redoop.flume.sink.avro.kafka.parsers.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;

/**
 * Powerspace code created by lvaldes on 24/12/15. All rights reserved.
 */
public class ParsingKafkaAvroSink extends KafkaAvroSink {
    public static final String PARSER_CLASS = "parser.class";

    private Parser parser;
    private static final Logger log = LoggerFactory.getLogger(KafkaAvroSink.class);

    protected IndexedRecord buildRecord(byte[] body) throws IOException {
        String line = new String(body);
        HashMap<String, Object> map = parser.parse(line);

        return KafkaAvroSinkUtil.recordFromMap(schema, map);
    }

    protected void instanciateParser() {
        try {
            parser = (Parser) Class.forName(props.getProperty(PARSER_CLASS)).newInstance();
        } catch (InstantiationException | ClassNotFoundException | IllegalAccessException e) {
            log.error("Exception while loading parser", e);
            stop();
        }
    }

    public void configure(Context context) {
        super.configure(context);

        instanciateParser();
    }
}
