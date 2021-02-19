package com.dtstack.flink.sql.sink.kafka.table;

import com.google.common.collect.Maps;
import org.junit.Test;

import java.util.Map;

public class KafkaSinkParserTest {

    @Test
    public void getTableInfo(){
        KafkaSinkParser kafkaSinkParser = new KafkaSinkParser();
        Map prop =  Maps.newHashMap();
        prop.put(KafkaSinkTableInfo.TYPE_KEY.toLowerCase(), "kafka");
        prop.put(KafkaSinkTableInfo.SINK_DATA_TYPE.toLowerCase(), "json");
        prop.put(KafkaSinkTableInfo.SCHEMA_STRING_KEY.toLowerCase(), "kafka");
        prop.put(KafkaSinkTableInfo.CSV_FIELD_DELIMITER_KEY.toLowerCase(), "kafka");
        prop.put(KafkaSinkTableInfo.BOOTSTRAPSERVERS_KEY.toLowerCase(), "kafka");
        prop.put(KafkaSinkTableInfo.TOPIC_KEY.toLowerCase(), "kafka");
        prop.put(KafkaSinkTableInfo.ENABLE_KEY_PARTITION_KEY.toLowerCase(), "kafka");
        prop.put(KafkaSinkTableInfo.PARALLELISM_KEY.toLowerCase(), "1");
        prop.put("kafka.test", "1");
        kafkaSinkParser.getTableInfo("tablea", "a varchar as a", prop);
    }
}
