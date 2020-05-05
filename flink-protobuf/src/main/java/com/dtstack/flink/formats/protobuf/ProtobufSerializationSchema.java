package com.dtstack.flink.formats.protobuf;

import org.apache.flink.api.common.serialization.SerializationSchema;

import com.google.protobuf.Message;

public class ProtobufSerializationSchema<T extends Message> implements SerializationSchema<T> {

    @Override
    public byte[] serialize(T t) {
        return t.toByteArray();
    }

}
