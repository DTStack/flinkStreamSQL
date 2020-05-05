package com.dtstack.flink.table.descriptors;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FormatDescriptorValidator;

/**
 * Validator for {@link Protobuf}.
 */
public class ProtobufValidator extends FormatDescriptorValidator {

    public static final String FORMAT_TYPE_VALUE = "protobuf";
    public static final String FORMAT_MESSAGE_CLASS = "format.message-class";
    public static final String FORMAT_PROTOBUF_DESCRIPTOR_HTTP_GET_URL = "format.protobuf-descriptor-http-get-url";

    @Override
    public void validate(DescriptorProperties properties) {
        super.validate(properties);
        final boolean hasMessageClass = properties.containsKey(FORMAT_MESSAGE_CLASS);
        final boolean hasProtobufDescriptorHttpGetUrl = properties.containsKey(FORMAT_PROTOBUF_DESCRIPTOR_HTTP_GET_URL);
        if (hasMessageClass && hasProtobufDescriptorHttpGetUrl) {
            throw new ValidationException("A definition of both a  Protobuf message class and Protobuf get descriptor http url  is not allowed.");
        } else if (hasMessageClass) {
            properties.validateString(FORMAT_MESSAGE_CLASS, false, 1);
        } else if (hasProtobufDescriptorHttpGetUrl) {
            properties.validateString(FORMAT_PROTOBUF_DESCRIPTOR_HTTP_GET_URL, false, 1);
        } else {
            throw new ValidationException("A definition of an Protobuf message class or Protobuf get descriptor http url is required.");
        }
    }
}
