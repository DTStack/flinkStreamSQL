package com.dtstack.flink.formats.protobuf;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.util.Preconditions;

import com.dtstack.flink.MoreSuppliers;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Message;

/**
 *  Deserialization schema that deserializes from Protobuf binary format.
 */
public class ProtobufDeserializationSchema<T extends Message> extends AbstractDeserializationSchema<T> {

    private static final long serialVersionUID = 2098447220136965L;

    /** Class to deserialize to. */
    private Class<T> messageClazz;

    /** DescriptorBytes in case of Message for serialization purpose. */
    private byte[] descriptorBytes;

    /** Descriptor generated from DescriptorBytes */
    private transient Descriptors.Descriptor descriptor;

    /** Default instance for this T message */
    private transient T defaultInstance;

    /**
     * Creates {@link ProtobufDeserializationSchema} that produces {@link Message} using provided schema.
     *
     * @param descriptorBytes of produced messages
     * @return deserialized message in form of {@link Message}
     */
    public static ProtobufDeserializationSchema<Message> forGenericMessage(byte[] descriptorBytes) {
        return new ProtobufDeserializationSchema<>(Message.class, descriptorBytes);
    }

    /**
     * Creates {@link ProtobufDeserializationSchema} that produces classes that were generated from protobuf schema.
     *
     * @param messageClazz class of message to be produced
     * @return deserialized message
     */
    public static <T extends GeneratedMessageV3> ProtobufDeserializationSchema<T> forSpecificMessage(Class<T> messageClazz) {
        return new ProtobufDeserializationSchema<>(messageClazz, null);
    }

    /**
     * Creates a Protobuf deserialization schema.
     *
     * @param messageClazz class to which deserialize.
     * @param descriptorBytes descriptor to which deserialize.
     */
    @SuppressWarnings("unchecked")
    ProtobufDeserializationSchema(Class<T> messageClazz, byte[] descriptorBytes) {
        Preconditions.checkNotNull(messageClazz, "Protobuf message class must not be null.");
        this.messageClazz = messageClazz;
        this.descriptorBytes = descriptorBytes;
        if (null != this.descriptorBytes) {
            this.descriptor = ProtobufUtils.getDescriptor(descriptorBytes);
            this.defaultInstance = (T) DynamicMessage.newBuilder(this.descriptor).getDefaultInstanceForType();
        } else {
            this.descriptor = ProtobufUtils.getDescriptor(messageClazz);
            this.defaultInstance = ProtobufUtils.getDefaultInstance(messageClazz);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public T deserialize(byte[] bytes) throws IOException {
        // read message
        return (T) this.defaultInstance.newBuilderForType().mergeFrom(bytes);
    }

    @SuppressWarnings("unchecked")
    private void readObject(ObjectInputStream inputStream) throws ClassNotFoundException, IOException {
        this.messageClazz = (Class<T>) inputStream.readObject();
        this.descriptorBytes = MoreSuppliers.throwing(() -> ProtobufUtils.getBytes(inputStream));
        if (null != this.descriptorBytes) {
            this.descriptor = ProtobufUtils.getDescriptor(descriptorBytes);
            this.defaultInstance = (T) DynamicMessage.newBuilder(this.descriptor).getDefaultInstanceForType();
        } else {
            this.descriptor = ProtobufUtils.getDescriptor(messageClazz);
            this.defaultInstance = ProtobufUtils.getDefaultInstance(messageClazz);
        }
    }

    private void writeObject(ObjectOutputStream outputStream) throws IOException {
        outputStream.writeObject(this.messageClazz);
        outputStream.write(this.descriptorBytes);
    }


}
