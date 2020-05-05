package com.dtstack.flink.formats.protobuf;


import static com.google.protobuf.Descriptors.FieldDescriptor.Type.ENUM;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TimeZone;
import java.util.stream.Collectors;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.dtstack.flink.MoreSuppliers;
import com.dtstack.flink.formats.protobuf.typeutils.ProtobufSchemaConverter;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.MapEntry;
import com.google.protobuf.Message;

/**
 * Serialization schema that serializes {@link Row} into Protobuf bytes.
 *
 * <p>Serializes objects that are represented in (nested) Flink rows. It support types that
 * are compatible with Flink's Table & SQL API.
 *
 * <p>Note: Changes in this class need to be kept in sync with the corresponding runtime
 * class {@link ProtobufRowDeserializationSchema} and schema converter {@link com.dtstack.flink.formats.protobuf.typeutils.ProtobufSchemaConverter}.
 */
public class ProtobufRowSerializationSchema implements SerializationSchema<Row> {

    private static final long serialVersionUID = 2098447220136965L;

    /**
     * Used for time conversions from SQL types.
     */
    private static final TimeZone LOCAL_TZ = TimeZone.getDefault();

    /**
     * Protobuf message class for serialization. Might be null if message class is not available.
     */
    private Class<? extends Message> messageClazz;

    /**
     * DescriptorBytes for deserialization.
     */
    private byte[] descriptorBytes;

    /**
     * Type information describing the result type.
     */
    private transient RowTypeInfo typeInfo;

    private transient Descriptors.Descriptor descriptor;

    private transient Message defaultInstance;

    /**
     * Creates an Protobuf serialization schema for the given message class.
     *
     * @param messageClazz Protobuf message class used to serialize Flink's row to Protobuf's message
     */
    public ProtobufRowSerializationSchema(Class<? extends GeneratedMessageV3> messageClazz) {
        Preconditions.checkNotNull(messageClazz, "Protobuf message class must not be null.");
        this.messageClazz = messageClazz;
        this.descriptorBytes = null;
        this.descriptor = ProtobufUtils.getDescriptor(this.messageClazz);
        this.typeInfo = (RowTypeInfo) ProtobufSchemaConverter.convertToTypeInfo(this.messageClazz);
        this.defaultInstance = ProtobufUtils.getDefaultInstance(this.messageClazz);
    }

    /**
     * Creates an Protobuf serialization schema for the given descriptorBytes.
     *
     * @param descriptorBytes descriptorBytes used to serialize Flink's row to Protobuf's message
     */
    public ProtobufRowSerializationSchema(byte[] descriptorBytes) {
        Preconditions.checkNotNull(descriptorBytes, "Protobuf message class must not be null.");
        this.messageClazz = null;
        this.descriptorBytes = descriptorBytes;
        this.descriptor = ProtobufUtils.getDescriptor(this.descriptorBytes);
        this.typeInfo = (RowTypeInfo) ProtobufSchemaConverter.convertToTypeInfo(descriptorBytes);
        this.defaultInstance = ProtobufUtils.getDefaultInstance(descriptorBytes);
    }

    @Override
    public byte[] serialize(Row row) {
        try {
            // convert to message
            Message message = this.convertRowToProtobufMessage(row, this.typeInfo, this.descriptor);
            return message.toByteArray();
        } catch (Throwable e) {
            throw new RuntimeException("Failed to serialize row.", e);
        }
    }

    private Message convertRowToProtobufMessage(Row row, RowTypeInfo rowTypeInfo, Descriptors.Descriptor descriptor) {
        List<Descriptors.FieldDescriptor> fields = descriptor.getFields();
        final TypeInformation<?>[] fieldTypes = rowTypeInfo.getFieldTypes();
        final int length = fields.size();

        DynamicMessage.Builder dynamicMessageBuilder = DynamicMessage.newBuilder(descriptor);

        for (int i = 0; i < length; ++i) {
            Descriptors.FieldDescriptor fieldDescriptor = fields.get(i);
            dynamicMessageBuilder.setField(fieldDescriptor, convertRowTypeToProtobufType(fieldDescriptor, fieldTypes[i], row.getField(i)));

        }

        return dynamicMessageBuilder.build();
    }

    private Object convertRowTypeToProtobufType(Descriptors.GenericDescriptor genericDescriptor, TypeInformation<?> info, Object object) {
        if (null == object) {
            return null;
        }

        if (genericDescriptor instanceof Descriptors.Descriptor) {

            return convertRowToProtobufMessage((Row) object, (RowTypeInfo) info, (Descriptors.Descriptor) genericDescriptor);

        } else if (genericDescriptor instanceof Descriptors.FieldDescriptor) {

            Descriptors.FieldDescriptor fieldDescriptor = ((Descriptors.FieldDescriptor) genericDescriptor);

            // field
            switch (fieldDescriptor.getType()) {
                case INT32:
                case FIXED32:
                case UINT32:
                case SFIXED32:
                case SINT32:
                    // check for logical types
                    if (info instanceof ListTypeInfo) {
                        // list
                        List<Object> results = new ArrayList<>(((List<?>) object).size());
                        for (Object o : ((List<?>) object)) {
                            if (o instanceof Date) {
                                results.add(this.convertFromDate((Date) o));
                            } else if (o instanceof Time) {
                                results.add(this.convertFromTime((Time) o));
                            } else {
                                results.add(o);
                            }
                        }
                        return results;
                    } else {
                        if (object instanceof Date) {
                            return this.convertFromDate((Date) object);
                        } else if (object instanceof Time) {
                            return this.convertFromTime((Time) object);
                        } else {
                            return object;
                        }
                    }
                case INT64:
                case UINT64:
                case FIXED64:
                case SFIXED64:
                case SINT64:
                    // check for logical type
                    if (info instanceof ListTypeInfo) {
                        // list
                        List<Object> results = new ArrayList<>(((List<?>) object).size());
                        for (Object o : ((List<?>) object)) {
                            if (o instanceof Timestamp) {
                                results.add(convertFromTimestamp((Timestamp) o));
                            } else {
                                results.add(o);
                            }
                        }
                        return results;
                    } else {
                        if (object instanceof Timestamp) {
                            return convertFromTimestamp((Timestamp) object);
                        } else {
                            return object;
                        }
                    }
                case DOUBLE:
                case FLOAT:
                case BOOL:
                    if (info instanceof ListTypeInfo) {
                        // list
                        return new ArrayList<>((List<?>) object);
                    } else {
                        return object;
                    }
                case STRING:
                case ENUM:
                    if (info instanceof ListTypeInfo) {
                        // list
                        return new ArrayList<>((List<?>) object)
                                .stream()
                                .map((Object o) -> convertFromEnum(fieldDescriptor, o))
                                .collect(Collectors.toList());
                    } else {
                        return convertFromEnum(fieldDescriptor, object);
                    }
                case GROUP:
                case MESSAGE:
                    if (info instanceof ListTypeInfo) {
                        // list
                        final List<?> elements = (List<?>) object;

                        final List<Object> convertedElements = new ArrayList<>(elements.size());
                        TypeInformation<?> elementTypeInfo = ((ListTypeInfo) info).getElementTypeInfo();
                        for (Object element : elements) {
                            convertedElements.add(convertRowTypeToProtobufType(fieldDescriptor.getMessageType(), elementTypeInfo, element));
                        }
                        return convertedElements;

                    } else if (info instanceof MapTypeInfo) {
                        // map
                        final List<MapEntry<?, ?>> pbMapEntries = new ArrayList<>(((Map<?, ?>) object).size());
                        for (Map.Entry<?, ?> mapEntry : ((Map<?, ?>) object).entrySet()) {
                            pbMapEntries.add(MapEntry.newDefaultInstance(
                                    fieldDescriptor.getMessageType()
                                    , fieldDescriptor.getMessageType().getFields().get(0).getLiteType()
                                    , mapEntry.getKey()
                                    , fieldDescriptor.getMessageType().getFields().get(1).getLiteType()
                                    , convertRowTypeToProtobufType(fieldDescriptor.getMessageType().getFields().get(1)
                                            , ((MapTypeInfo) info).getValueTypeInfo()
                                            , mapEntry.getValue())));
                        }
                        return pbMapEntries;
                    } else if (info instanceof RowTypeInfo) {
                        // row
                        return convertRowToProtobufMessage((Row) object
                                , (RowTypeInfo) info
                                , fieldDescriptor.getMessageType());
                    }
                    throw new IllegalStateException("Message expected but was: " + object.getClass());
                case BYTES:
                    // check for logical type
                    if (object instanceof BigDecimal) {
                        return convertFromDecimal((BigDecimal) object);
                    }
                    return object;
            }
        }
        throw new RuntimeException("error");
    }

    private byte[] convertFromDecimal(BigDecimal decimal) {
        // byte array must contain the two's-complement representation of the
        // unscaled integer value in big-endian byte order
        return decimal.unscaledValue().toByteArray();
    }

    private int convertFromDate(Date date) {
        final long time = date.getTime();
        final long converted = time + (long) LOCAL_TZ.getOffset(time);
        return (int) (converted / 86400000L);
    }

    private int convertFromTime(Time date) {
        final long time = date.getTime();
        final long converted = time + (long) LOCAL_TZ.getOffset(time);
        return (int) (converted % 86400000L);
    }

    private long convertFromTimestamp(Timestamp date) {
        // adopted from Apache Calcite
        final long time = date.getTime();
        return time + (long) LOCAL_TZ.getOffset(time);
    }

    private Object convertFromEnum(Descriptors.FieldDescriptor fieldDescriptor, Object object) {
        if (ENUM == fieldDescriptor.getType()) {

            Descriptors.EnumDescriptor enumDescriptor = fieldDescriptor.getEnumType();

            Descriptors.EnumValueDescriptor enumValue = null;

            for (Descriptors.EnumValueDescriptor enumValueDescriptor : enumDescriptor.getValues()) {
                if (enumValueDescriptor.toString().equals(object)) {
                    enumValue = enumValueDescriptor;
                }
            }

            if (null != enumValue) {
                return enumValue;
            } else {
                throw new NoSuchElementException(String.format(fieldDescriptor.getFullName() + " enumValues has not such element [%s]", object));
            }
        } else {
            return object.toString();
        }
    }

    private void writeObject(ObjectOutputStream outputStream) throws IOException {
        outputStream.writeObject(this.messageClazz);
        outputStream.write(this.descriptorBytes);
    }

    @SuppressWarnings("unchecked")
    private void readObject(ObjectInputStream inputStream) throws ClassNotFoundException, IOException {
        this.messageClazz = (Class<? extends Message>) inputStream.readObject();
        this.descriptorBytes = MoreSuppliers.throwing(() -> ProtobufUtils.getBytes(inputStream));
        if (null != this.descriptorBytes) {
            this.descriptor = ProtobufUtils.getDescriptor(this.descriptorBytes);
            this.typeInfo = (RowTypeInfo) ProtobufSchemaConverter.convertToTypeInfo(this.descriptor);
            this.defaultInstance = DynamicMessage.newBuilder(this.descriptor).getDefaultInstanceForType();
        } else {
            this.descriptor = ProtobufUtils.getDescriptor(this.messageClazz);
            this.typeInfo = (RowTypeInfo) ProtobufSchemaConverter.convertToTypeInfo(this.messageClazz);
            this.defaultInstance = ProtobufUtils.getDefaultInstance(this.messageClazz);
        }
    }
}
