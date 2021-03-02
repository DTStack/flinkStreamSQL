package com.dtstack.flink.sql.sink.kafka.table;

import org.junit.Test;

public class KafkaSinkTableInfoTest {

    @Test
    public void testKafkaSinkTableInfo(){
        KafkaSinkTableInfo kafkaSinkTableInfo = new KafkaSinkTableInfo();
        kafkaSinkTableInfo.setType("kafka");
        kafkaSinkTableInfo.setTopic("a");
        kafkaSinkTableInfo.setBootstrapServers("localhost:9092");
        kafkaSinkTableInfo.setSinkDataType("avro");
        kafkaSinkTableInfo.setUpdateMode("append");
        kafkaSinkTableInfo.setSchemaString("{\"name\":\"channel\",\"type\":\"string\"}");
        kafkaSinkTableInfo.check();
    }
}
