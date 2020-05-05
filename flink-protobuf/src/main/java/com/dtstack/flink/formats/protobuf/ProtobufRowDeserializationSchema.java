package com.dtstack.flink.formats.protobuf;


import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.joda.time.DateTime;
import org.joda.time.DateTimeFieldType;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;

import com.dtstack.flink.MoreSuppliers;
import com.dtstack.flink.formats.protobuf.typeutils.ProtobufSchemaConverter;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.MapEntry;
import com.google.protobuf.Message;

public class ProtobufRowDeserializationSchema extends AbstractDeserializationSchema<Row> {
    /**
     * Used for time conversions into SQL types.
     */
    private static final TimeZone LOCAL_TZ = TimeZone.getDefault();

    /**
     * Protobuf message class for deserialization. Might be null if message class is not available.
     */
    private Class<? extends Message> messageClazz;

    /**
     * Protobuf serialization descriptorBytes
     */
    private byte[] descriptorBytes;

    /**
     * Protobuf serialization descriptor.
     */
    private transient Descriptors.Descriptor descriptor;

    /**
     * Type information describing the result type.
     */
    private transient RowTypeInfo typeInfo;

    /**
     * Protobuf defaultInstance for descriptor
     */
    private transient Message defaultInstance;

    /**
     * Creates a Protobuf deserialization descriptor for the given message class. Having the
     * concrete Protobuf message class might improve performance.
     *
     * @param messageClazz Protobuf message class used to deserialize Protobuf's message to Flink's row
     */
    public ProtobufRowDeserializationSchema(Class<? extends GeneratedMessageV3> messageClazz) {
        Preconditions.checkNotNull(messageClazz, "Protobuf message class must not be null.");
        this.messageClazz = messageClazz;
        this.descriptorBytes = null;
        this.descriptor = ProtobufUtils.getDescriptor(messageClazz);
        this.typeInfo = (RowTypeInfo) ProtobufSchemaConverter.convertToTypeInfo(messageClazz);
        this.defaultInstance = ProtobufUtils.getDefaultInstance(messageClazz);
    }

    /**
     * Creates a Protobuf deserialization descriptor for the given Protobuf descriptorBytes.
     *
     * @param descriptorBytes Protobuf descriptorBytes to deserialize Protobuf's message to Flink's row
     */
    public ProtobufRowDeserializationSchema(byte[] descriptorBytes) {
        Preconditions.checkNotNull(descriptorBytes, "Protobuf descriptorBytes must not be null.");
        this.messageClazz = null;
        this.descriptorBytes = descriptorBytes;
        this.descriptor = ProtobufUtils.getDescriptor(descriptorBytes);
        this.typeInfo = (RowTypeInfo) ProtobufSchemaConverter.convertToTypeInfo(this.descriptor);
        this.defaultInstance = DynamicMessage.newBuilder(this.descriptor).getDefaultInstanceForType();
    }

    @Override
    public Row deserialize(byte[] bytes) throws IOException {
        try {
            Message message = this.defaultInstance
                    .newBuilderForType()
                    .mergeFrom(bytes)
                    .build();
            return convertProtobufMessageToRow(message, this.typeInfo);
        } catch (Exception e) {
            throw new IOException("Failed to deserialize Protobuf message.", e);
        }
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return this.typeInfo;
    }

    // --------------------------------------------------------------------------------------------

    private Row convertProtobufMessageToRow(Message message, RowTypeInfo rowTypeInfo) {
        final List<Descriptors.FieldDescriptor> fields = message.getDescriptorForType().getFields();
        final TypeInformation<?>[] fieldTypes = rowTypeInfo.getFieldTypes();
        final int length = fields.size();
        final Row row = new Row(length);
        for (int i = 0; i < length; i++) {
            final Descriptors.FieldDescriptor field = fields.get(i);
            row.setField(i, convertProtobufTypeToRowType(field, fieldTypes[i], message.getField(field)));
        }
        return row;
    }

    @SuppressWarnings("unchecked")
    private Object convertProtobufTypeToRowType(Descriptors.GenericDescriptor genericDescriptor, TypeInformation<?> info, Object object) {
        // we perform the conversion based on descriptor information but enriched with pre-computed
        // type information where useful (i.e., for list)

        if (object == null) {
            return null;
        }

        if (genericDescriptor instanceof Descriptors.Descriptor) {

            return convertProtobufMessageToRow((Message) object, (RowTypeInfo) info);

        } else if (genericDescriptor instanceof Descriptors.FieldDescriptor) {

            Descriptors.FieldDescriptor fieldDescriptor = ((Descriptors.FieldDescriptor) genericDescriptor);

            // field
            switch (fieldDescriptor.getType()) {
                case INT32:
                case FIXED32:
                case UINT32:
                case SFIXED32:
                case SINT32:
                    if (info instanceof ListTypeInfo) {
                        // list
                        TypeInformation<?> elementTypeInfo = ((ListTypeInfo) info).getElementTypeInfo();

                        if (Types.SQL_DATE == elementTypeInfo) {
                            return new ArrayList<>(((List<?>) object))
                                    .stream()
                                    .map(this::convertToDate)
                                    .collect(Collectors.toList());
                        } else if (Types.SQL_TIME == info) {
                            return new ArrayList<>(((List<?>) object))
                                    .stream()
                                    .map(this::convertToTime)
                                    .collect(Collectors.toList());
                        } else {
                            return new ArrayList<>(((List<?>) object));
                        }
                    } else {
                        if (Types.SQL_DATE == info) {
                            return convertToDate(object);
                        } else if (Types.SQL_TIME == info) {
                            return convertToTime(object);
                        } else {

                            return object;
                        }
                    }
                case INT64:
                case UINT64:
                case FIXED64:
                case SFIXED64:
                case SINT64:
                    if (info instanceof ListTypeInfo) {
                        // list
                        TypeInformation<?> elementTypeInfo = ((ListTypeInfo) info).getElementTypeInfo();

                        if (Types.SQL_TIMESTAMP == elementTypeInfo) {
                            return new ArrayList<>(((List<?>) object))
                                    .stream()
                                    .map(this::convertToTimestamp)
                                    .collect(Collectors.toList());
                        } else {
                            return new ArrayList<>(((List<?>) object));
                        }
                    } else {
                        if (Types.SQL_TIMESTAMP == info) {
                            return convertToTimestamp(object);
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
                                .map(Object::toString)
                                .collect(Collectors.toList());
                    } else {
                        return object.toString();
                    }
                case GROUP:
                case MESSAGE:
                    if (info instanceof ListTypeInfo) {
                        // list
                        final List<?> elements = (List<?>) object;

                        final List<Object> convertedElements = new ArrayList<>(elements.size());
                        TypeInformation<?> elementTypeInfo = ((ListTypeInfo) info).getElementTypeInfo();
                        for (Object element : elements) {
                            convertedElements.add(convertProtobufTypeToRowType(fieldDescriptor.getMessageType(), elementTypeInfo, element));
                        }
                        return convertedElements;
                    } else if (info instanceof MapTypeInfo) {
                        // map todo map的key暂时只支持string
                        final MapTypeInfo<?, ?> mapTypeInfo = (MapTypeInfo<?, ?>) info;
                        final Map<String, Object> convertedMap = new HashMap<>();
                        final List<Message> mapList = (List<Message>) object;

                        for (Message entry : mapList) {
                            if (entry instanceof MapEntry) {
                                convertedMap.put(
                                        ((MapEntry) entry).getKey().toString(),
                                        convertProtobufTypeToRowType(fieldDescriptor.getMessageType().getFields().get(1)
                                                , mapTypeInfo.getValueTypeInfo()
                                                , ((MapEntry) entry).getValue()));
                            } else if (entry instanceof DynamicMessage) {
                                DynamicMessage dynamicMessage = (DynamicMessage) entry;
                                Object key = dynamicMessage.getField(fieldDescriptor.getMessageType().getFields().get(0));
                                Object value = dynamicMessage.getField(fieldDescriptor.getMessageType().getFields().get(1));

                                convertedMap.put(
                                        key.toString(),
                                        convertProtobufTypeToRowType(fieldDescriptor.getMessageType().getFields().get(1)
                                                , mapTypeInfo.getValueTypeInfo()
                                                , value));
                            }
                        }
                        return convertedMap;
                    } else if (info instanceof RowTypeInfo) {
                        // row
                        return convertProtobufMessageToRow((Message) object, (RowTypeInfo) info);
                    }
                    throw new IllegalStateException("Message expected but was: " + object.getClass());
                case BYTES:
                    final byte[] bytes = ((ByteString) object).toByteArray();
                    if (Types.BIG_DEC == info) {
                        return convertToDecimal(bytes);
                    }
                    return bytes;
            }
        }

        throw new IllegalArgumentException("Unsupported Protobuf type '" + genericDescriptor.getName() + "'.");

    }

    private BigDecimal convertToDecimal(byte[] bytes) {
        return new BigDecimal(new BigInteger(bytes));
    }

    private Date convertToDate(Object object) {
        final long millis;
        if (object instanceof Integer) {
            final Integer value = (Integer) object;
            // adopted from Apache Calcite
            final long t = (long) value * 86400000L;
            millis = t - (long) LOCAL_TZ.getOffset(t);
        } else {
            // use 'provided' Joda time
            final LocalDate value = (LocalDate) object;
            millis = value.toDate().getTime();
        }
        return new Date(millis);
    }

    private Time convertToTime(Object object) {
        final long millis;
        if (object instanceof Integer) {
            millis = (Integer) object;
        } else {
            // use 'provided' Joda time
            final LocalTime value = (LocalTime) object;
            millis = value.get(DateTimeFieldType.millisOfDay());
        }
        return new Time(millis - LOCAL_TZ.getOffset(millis));
    }

    private Timestamp convertToTimestamp(Object object) {
        final long millis;
        if (object instanceof Long) {
            millis = (Long) object;
        } else {
            // use 'provided' Joda time
            final DateTime value = (DateTime) object;
            millis = value.toDate().getTime();
        }
        return new Timestamp(millis - LOCAL_TZ.getOffset(millis));
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
