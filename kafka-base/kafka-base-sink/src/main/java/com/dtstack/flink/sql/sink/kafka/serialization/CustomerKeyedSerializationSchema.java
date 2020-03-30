package com.dtstack.flink.sql.sink.kafka.serialization;


import com.dtstack.flink.sql.format.SerializationMetricWrapper;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class CustomerKeyedSerializationSchema implements KeyedSerializationSchema<Tuple2<Boolean,Row>> {

    private static final Logger LOG = LoggerFactory.getLogger(CustomerKeyedSerializationSchema.class);

    private static final AtomicLong COUNTER = new AtomicLong(0L);

    private static final long serialVersionUID = 1L;
    private final SerializationMetricWrapper serializationMetricWrapper;
    private String[] partitionKeys;
    private ObjectMapper mapper = null;

    public CustomerKeyedSerializationSchema(SerializationMetricWrapper serializationMetricWrapper, String[] partitionKeys) {
        this.serializationMetricWrapper = serializationMetricWrapper;
        this.partitionKeys = partitionKeys;
        this.mapper = new ObjectMapper();
    }

    @Override
    public byte[] serializeKey(Tuple2<Boolean,Row> element) {
        if (partitionKeys == null || partitionKeys.length <= 0) {
            return null;
        }
        SerializationSchema<Tuple2<Boolean,Row>> serializationSchema = serializationMetricWrapper.getSerializationSchema();
        if (serializationSchema instanceof JsonTupleSerializationSchema) {
            return serializeJsonKey((JsonTupleSerializationSchema) serializationSchema, element);
        }
        return null;
    }

    @Override
    public byte[] serializeValue(Tuple2<Boolean,Row> element) {
        return this.serializationMetricWrapper.serialize(element);
    }

    @Override
    public String getTargetTopic(Tuple2<Boolean,Row> element) {
        return null;
    }

    private byte[] serializeJsonKey(JsonTupleSerializationSchema jsonTupleSerializationSchema, Tuple2<Boolean,Row> element) {
        try {
            byte[] data = jsonTupleSerializationSchema.serialize(element);
            ObjectNode objectNode = mapper.readValue(data, ObjectNode.class);
            StringBuilder sb = new StringBuilder();
            for (String key : partitionKeys) {
                if (objectNode.has(key)) {
                    sb.append(objectNode.get(key.trim()));
                }
            }
            return sb.toString().getBytes();
        } catch (Exception e) {
            if (COUNTER.getAndIncrement() % 1000 == 0) {
                LOG.error("serializeJsonKey error", e);
            }
        }
        return null;
    }
}
