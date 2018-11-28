package com.dtstack.flink.sql.source.kafka;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.types.Row;

import static java.nio.charset.StandardCharsets.UTF_8;

public class CustomerCommonDeserialization implements KeyedDeserializationSchema<Row>
{
    public static final String[] KAFKA_COLUMNS = new String[] {"TOPIC", "MESSAGEKEY", "MESSAGE", "PARTITION", "OFFSET"};

    @Override
    public boolean isEndOfStream(Row nextElement)
    {
        return false;
    }

    @Override
    public Row deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset)
    {
        return Row.of(
                topic, //topic
                messageKey == null ? null : new String(messageKey, UTF_8), //key
                new String(message, UTF_8), //message
                partition,
                offset
        );
    }

    public TypeInformation<Row> getProducedType()
    {
        TypeInformation<?>[] types = new TypeInformation<?>[] {
                TypeExtractor.createTypeInfo(String.class),
                TypeExtractor.createTypeInfo(String.class), //createTypeInformation[String]
                TypeExtractor.createTypeInfo(String.class),
                Types.INT,
                Types.LONG
        };
        return new RowTypeInfo(types, KAFKA_COLUMNS);
    }
}
