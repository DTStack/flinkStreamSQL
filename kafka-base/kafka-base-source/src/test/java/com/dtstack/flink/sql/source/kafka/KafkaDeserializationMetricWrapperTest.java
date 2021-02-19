package com.dtstack.flink.sql.source.kafka;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.internal.KafkaConsumerThread;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;

import static org.mockito.Mockito.*;

public class KafkaDeserializationMetricWrapperTest {
    private KafkaDeserializationMetricWrapper kafkaDeserializationMetricWrapper;

    @Before
    public void init() {
        TypeInformation<Row> typeInfo = mock(TypeInformation.class);
        DeserializationSchema<Row> deserializationSchema = mock(DeserializationSchema.class);
        Calculate calculate = mock(Calculate.class);
        kafkaDeserializationMetricWrapper = new KafkaDeserializationMetricWrapper(typeInfo, deserializationSchema, calculate);
    }

    @Test
    public void beforeDeserialize() throws IOException {
        //kafkaDeserializationMetricWrapper.beforeDeserialize();
    }

}
