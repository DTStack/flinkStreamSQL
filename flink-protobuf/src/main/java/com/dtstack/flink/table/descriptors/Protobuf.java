package com.dtstack.flink.table.descriptors;

import java.util.Map;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FormatDescriptor;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.Message;

/**
 * Format descriptor for Apache Protobuf messages.
 */
@PublicEvolving
public class Protobuf extends FormatDescriptor {

    private Class<? extends Message> messageClass;
    private String protobufDescriptorHttpGetUrl;

    public Protobuf() {
        super(ProtobufValidator.FORMAT_TYPE_VALUE, 1);
    }

    /**
     * Sets the class of the Protobuf message.
     *
     * @param messageClass class of the Protobuf message.
     */
    public Protobuf messageClass(Class<? extends Message> messageClass) {
        Preconditions.checkNotNull(messageClass);
        this.messageClass = messageClass;
        return this;
    }

    /**
     * Sets the Protobuf for protobuf messages.
     *
     * @param protobufDescriptorHttpGetUrl protobuf descriptor http get url
     */
    public Protobuf protobufDescriptorHttpGetUrl(String protobufDescriptorHttpGetUrl) {
        Preconditions.checkNotNull(protobufDescriptorHttpGetUrl);
        this.protobufDescriptorHttpGetUrl = protobufDescriptorHttpGetUrl;
        return this;
    }

    @Override
    protected Map<String, String> toFormatProperties() {
        final DescriptorProperties properties = new DescriptorProperties();

        if (null != messageClass) {
            properties.putClass(ProtobufValidator.FORMAT_MESSAGE_CLASS, messageClass);
        }
        if (null != protobufDescriptorHttpGetUrl) {
            properties.putString(ProtobufValidator.FORMAT_PROTOBUF_DESCRIPTOR_HTTP_GET_URL, protobufDescriptorHttpGetUrl);
        }

        return properties.asMap();
    }
}
