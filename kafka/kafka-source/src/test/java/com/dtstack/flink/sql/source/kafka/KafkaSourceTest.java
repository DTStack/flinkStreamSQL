package com.dtstack.flink.sql.source.kafka;

import com.dtstack.flink.sql.source.kafka.table.KafkaSourceTableInfo;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.junit.Test;
import org.mockito.ArgumentMatcher;

import static org.mockito.Mockito.*;

public class KafkaSourceTest {

    @Test
    public void mockGenStreamSource(){
        KafkaSourceTableInfo kafkaSourceTableInfo = mock(KafkaSourceTableInfo.class);
        when(kafkaSourceTableInfo.getTopic()).thenReturn("topic");
        when(kafkaSourceTableInfo.getGroupId()).thenReturn("groupId");
        when(kafkaSourceTableInfo.getBootstrapServers()).thenReturn("localhost");
        when(kafkaSourceTableInfo.getKafkaParamKeys()).thenReturn(Sets.newHashSet("aa"));
        when(kafkaSourceTableInfo.getKafkaParam("aa")).thenReturn("xx");
        when(kafkaSourceTableInfo.getFields()).thenReturn(new String[]{"aa"});
        when(kafkaSourceTableInfo.getFieldClasses()).thenReturn(new Class[]{String.class});
        when(kafkaSourceTableInfo.getParallelism()).thenReturn(1);
        when(kafkaSourceTableInfo.getName()).thenReturn("roc");
        when(kafkaSourceTableInfo.getOffsetReset()).thenReturn("1");
        when(kafkaSourceTableInfo.getSourceDataType()).thenReturn("json");

        DataStreamSource dataStreamSource = mock(DataStreamSource.class);

        StreamExecutionEnvironment env = mock(StreamExecutionEnvironment.class);
        when(env.addSource(argThat(new ArgumentMatcher<SourceFunction<? extends Object>>() {
            @Override
            public boolean matches(SourceFunction<?> sourceFunction) {
                return true;
            }
        }), argThat(new ArgumentMatcher<String>() {
            @Override
            public boolean matches(String s) {
                return true;
            }
        }), argThat(new ArgumentMatcher<TypeInformation>() {
            @Override
            public boolean matches(TypeInformation typeInformation) {
                return true;
            }
        }))).thenReturn(dataStreamSource);

        StreamTableEnvironment tableEnv = mock(StreamTableEnvironment.class);

        KafkaSource kafkaSource = new KafkaSource();
        KafkaSource kafkaSourceSpy = spy(kafkaSource);
        kafkaSourceSpy.genStreamSource(kafkaSourceTableInfo, env, tableEnv);
    }

}

