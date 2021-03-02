package com.dtstack.flink.sql.source.kafka;

import com.dtstack.flink.sql.source.kafka.table.KafkaSourceParser;
import com.google.common.collect.Maps;
import org.junit.Test;

import java.util.Map;

import static org.mockito.Mockito.*;

public class KafkaSourceParserTest {

    @Test
    public void mockGetTableInfo() throws Exception {
        String tableName = "table";
        String fieldsInfo = "aa varchar";
        Map<String, Object> props = Maps.newHashMap();
        props.put("type", "kafka10");
        props.put("parallelism", "1");
        props.put("bootstrapservers", "localhost");
        props.put("groupid", "groupId");
        props.put("topic", "topic");
        props.put("offsetreset", "1atest");
        props.put("topicsspattern", "false");
        props.put("sourcedataType", "json");
        KafkaSourceParser kafkaSourceParser = new KafkaSourceParser();
        KafkaSourceParser kafkaSourceParserSpy = spy(kafkaSourceParser);
        kafkaSourceParserSpy.getTableInfo(tableName, fieldsInfo, props);
        verify(kafkaSourceParserSpy).getTableInfo(tableName, fieldsInfo, props);
    }
}
