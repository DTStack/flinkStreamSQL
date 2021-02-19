package com.dtstack.flink.sql.sink.kafka;

import com.dtstack.flink.sql.format.SerializationMetricWrapper;
import com.dtstack.flink.sql.sink.kafka.serialization.CustomerKeyedSerializationSchema;
import com.dtstack.flink.sql.sink.kafka.serialization.JsonTupleSerializationSchema;
import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CustomerKeyedSerializationSchemaTest {

    @Test
    public void testCustomerKeyedSerializationSchema(){
        SerializationMetricWrapper serializationMetricWrapper = mock(SerializationMetricWrapper.class);
        JsonTupleSerializationSchema jsonCRowSerializationSchema = mock(JsonTupleSerializationSchema.class);
        when(serializationMetricWrapper.getSerializationSchema()).thenReturn(jsonCRowSerializationSchema);
        when(jsonCRowSerializationSchema.serialize(any())).thenReturn("{\"a\":\"n\"}".getBytes());
        CustomerKeyedSerializationSchema customerKeyedSerializationSchema = new CustomerKeyedSerializationSchema(serializationMetricWrapper, Lists.newArrayList("a").toArray(new String[1]));
        Tuple2 cRow = mock(Tuple2.class);
        customerKeyedSerializationSchema.serializeKey(cRow);
        customerKeyedSerializationSchema.getTargetTopic(cRow);
        customerKeyedSerializationSchema.serializeValue(cRow);
    }
}
