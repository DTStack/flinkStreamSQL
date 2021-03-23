package com.dtstack.flink.sql.source.kafka;

import com.dtstack.flink.sql.sink.kafka.KafkaSink;
import com.dtstack.flink.sql.sink.kafka.table.KafkaSinkTableInfo;
import com.google.common.collect.Sets;
import org.junit.Test;

import static org.mockito.Mockito.*;

public class KafkaSinkTest {

    @Test
    public void mockGenStreamSink(){
        KafkaSinkTableInfo kafkaSinkTableInfo = mock(KafkaSinkTableInfo.class);
        when(kafkaSinkTableInfo.getName()).thenReturn("roc");
        when(kafkaSinkTableInfo.getTopic()).thenReturn("topic");
        when(kafkaSinkTableInfo.getBootstrapServers()).thenReturn("localhost");
        when(kafkaSinkTableInfo.getKafkaParamKeys()).thenReturn(Sets.newHashSet("aa"));
        when(kafkaSinkTableInfo.getKafkaParam("aa")).thenReturn("xx");
        when(kafkaSinkTableInfo.getPartitionKeys()).thenReturn(null);
        when(kafkaSinkTableInfo.getFields()).thenReturn(new String[]{"aa"});
        when(kafkaSinkTableInfo.getFieldClasses()).thenReturn(new Class[]{String.class});
        when(kafkaSinkTableInfo.getParallelism()).thenReturn(1);
        when(kafkaSinkTableInfo.getSinkDataType()).thenReturn("json");
        when(kafkaSinkTableInfo.getUpdateMode()).thenReturn("append");

        KafkaSink kafkaSink = new KafkaSink();
        KafkaSink kafkaSinkSpy = spy(kafkaSink);
        kafkaSinkSpy.genStreamSink(kafkaSinkTableInfo);
    }
}
