package com.dtstack.flink.sql.source.kafka;

import com.dtstack.flink.sql.dirtyManager.manager.DirtyDataManager;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.internal.KafkaConsumerThread;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;

public class KafkaDeserializationMetricWrapperTest {
    private KafkaDeserializationMetricWrapper kafkaDeserializationMetricWrapper;

    @Before
    public void init() {
        Map<String, Object> dirtyMap = new HashMap<>();
        dirtyMap.put("type", "console");
        // 多少条数据打印一次
        dirtyMap.put("printLimit", "100");
        dirtyMap.put("url", "jdbc:mysql://localhost:3306/tiezhu");
        dirtyMap.put("userName", "root");
        dirtyMap.put("password", "abc123");
        dirtyMap.put("isCreateTable", "false");
        // 多少条数据写入一次
        dirtyMap.put("batchSize", "1");
        dirtyMap.put("tableName", "dirtyData");
        dirtyMap.put("pluginLoadMode", "localTest");
        TypeInformation<Row> typeInfo = mock(TypeInformation.class);
        DeserializationSchema<Row> deserializationSchema = mock(DeserializationSchema.class);
        Calculate calculate = mock(Calculate.class);
//        kafkaDeserializationMetricWrapper = new KafkaDeserializationMetricWrapper(typeInfo, deserializationSchema, calculate, DirtyDataManager.newInstance(dirtyMap));
    }

    @Test
    public void beforeDeserialize() throws IOException {
        //kafkaDeserializationMetricWrapper.beforeDeserialize();
    }

}
