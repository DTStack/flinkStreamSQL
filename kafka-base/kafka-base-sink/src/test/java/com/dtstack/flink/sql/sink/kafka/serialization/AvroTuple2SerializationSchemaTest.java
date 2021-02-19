package com.dtstack.flink.sql.sink.kafka.serialization;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.mockito.PowerMockito;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AvroTuple2SerializationSchemaTest {

    private AvroTuple2SerializationSchema avroTuple2SerializationSchema;

    @Before
    public void init(){
        String schemaString = "{\"type\":\"record\",\"name\":\"MyResult\",\"fields\":[{\"name\":\"channel\",\"type\":\"string\"}]}";
        avroTuple2SerializationSchema = new AvroTuple2SerializationSchema(schemaString,"upsert");
    }

    @Test
    public void serialize(){
        Row row = new Row(1);
        row.setField(0, "1");
        avroTuple2SerializationSchema.serialize(new Tuple2<>(true, row));
        avroTuple2SerializationSchema.equals(avroTuple2SerializationSchema);
        avroTuple2SerializationSchema.equals(null);
    }

    @Test
    public void convertFlinkType() throws Exception {
        Schema schema = PowerMockito.mock(Schema.class);
        when(schema.getType()).thenReturn(Schema.Type.RECORD);
        Method method = AvroTuple2SerializationSchema.class.getDeclaredMethod("convertFlinkType", Schema.class, Object.class);
        method.setAccessible(true);
        method.invoke(avroTuple2SerializationSchema,schema, null);
        Row row = new Row(1);
        row.setField(0, "1");
        when(schema.getFields()).thenReturn(Lists.newArrayList());
        method.invoke(avroTuple2SerializationSchema,schema, row);

        when(schema.getType()).thenReturn(Schema.Type.ENUM);
        method.invoke(avroTuple2SerializationSchema,schema, "test");

        Schema schema1 = mock(Schema.class);
        when(schema1.getType()).thenReturn(Schema.Type.ARRAY);
        when(schema1.getElementType()).thenReturn(schema);
        method.invoke(avroTuple2SerializationSchema,schema, new int[]{1});

        Schema schema2 = mock(Schema.class);
        when(schema2.getType()).thenReturn(Schema.Type.ARRAY);
        when(schema2.getValueType()).thenReturn(schema);
        Map meta = Maps.newHashMap();
        meta.put("key", "value");
        method.invoke(avroTuple2SerializationSchema,schema, meta);

        when(schema.getType()).thenReturn(Schema.Type.UNION);
        when(schema.getTypes()).thenReturn(Lists.newArrayList());
        method.invoke(avroTuple2SerializationSchema,schema,  "1".getBytes());

        when(schema.getType()).thenReturn(Schema.Type.FIXED);
        method.invoke(avroTuple2SerializationSchema,schema,  "1".getBytes());

        when(schema.getType()).thenReturn(Schema.Type.STRING);
        method.invoke(avroTuple2SerializationSchema,schema, new BigDecimal("1"));

        when(schema.getType()).thenReturn(Schema.Type.BYTES);
        method.invoke(avroTuple2SerializationSchema,schema, "1".getBytes());

        when(schema.getType()).thenReturn(Schema.Type.INT);
        method.invoke(avroTuple2SerializationSchema,schema, 1);

        when(schema.getType()).thenReturn(Schema.Type.LONG);
        method.invoke(avroTuple2SerializationSchema,schema,  1);

        when(schema.getType()).thenReturn(Schema.Type.BOOLEAN);
        method.invoke(avroTuple2SerializationSchema,schema, true);
    }

    @Test
    public  void convertFromDecimal() throws Exception {
        Schema schema = PowerMockito.mock(Schema.class);
        LogicalTypes.Decimal decimalType = mock(LogicalTypes.Decimal.class);
        when(schema.getLogicalType()).thenReturn(decimalType);
        when(decimalType.getScale()).thenReturn(1);
        Method method = AvroTuple2SerializationSchema.class.getDeclaredMethod("convertFromDecimal", Schema.class, BigDecimal.class);
        method.setAccessible(true);
        method.invoke(avroTuple2SerializationSchema,schema, new BigDecimal("1"));
    }

    @Test
    public  void convertFromDate() throws Exception {
        Schema schema = PowerMockito.mock(Schema.class);
        LogicalTypes.Date date = LogicalTypes.date();
        when(schema.getLogicalType()).thenReturn(date);
        Method method = AvroTuple2SerializationSchema.class.getDeclaredMethod("convertFromDate", Schema.class, Date.class);
        method.setAccessible(true);
        method.invoke(avroTuple2SerializationSchema,schema, new Date(System.currentTimeMillis()));
    }
    @Test
    public  void convertFromTime() throws Exception {
        Schema schema = PowerMockito.mock(Schema.class);
        when(schema.getLogicalType()).thenReturn( LogicalTypes.timeMillis());
        LogicalTypes.TimeMillis date = LogicalTypes.timeMillis();
        Method method = AvroTuple2SerializationSchema.class.getDeclaredMethod("convertFromTime", Schema.class, Time.class);
        method.setAccessible(true);
        method.invoke(avroTuple2SerializationSchema, schema, new Time(System.currentTimeMillis()));
    }
    @Test
    public  void convertFromTimestamp() throws Exception {
        Schema schema = PowerMockito.mock(Schema.class);
        LogicalTypes.TimestampMillis date = LogicalTypes.timestampMillis();
        when(schema.getLogicalType()).thenReturn(date);
        Method method = AvroTuple2SerializationSchema.class.getDeclaredMethod("convertFromTimestamp", Schema.class, Timestamp.class);
        method.setAccessible(true);
        method.invoke(avroTuple2SerializationSchema,schema, new Timestamp(System.currentTimeMillis()));
    }

}
