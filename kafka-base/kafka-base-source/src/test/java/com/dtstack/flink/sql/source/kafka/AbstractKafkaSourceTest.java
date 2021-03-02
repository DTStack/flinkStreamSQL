package com.dtstack.flink.sql.source.kafka;

import com.dtstack.flink.sql.source.kafka.table.KafkaSourceTableInfo;
import com.dtstack.flink.sql.table.AbstractSourceTableInfo;
import com.google.common.collect.Lists;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

public class AbstractKafkaSourceTest {

    private AbstractKafkaSource kafkaSource;

    @Before
    public void init() {
        kafkaSource = new AbstractKafkaSource() {
            @Override
            public Table genStreamSource(AbstractSourceTableInfo sourceTableInfo, StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
                return null;
            }
        };
    }

    @Test
    public void getKafkaProperties() {
        KafkaSourceTableInfo kafkaSourceTableInfo = new KafkaSourceTableInfo();
        kafkaSourceTableInfo.setBootstrapServers("localhost");
        kafkaSourceTableInfo.setOffsetReset("1");
        kafkaSourceTableInfo.setGroupId("1");
        kafkaSourceTableInfo.setPrimaryKeys(Lists.newArrayList("a"));
        kafkaSource.getKafkaProperties(kafkaSourceTableInfo);
    }

    @Test
    public void generateOperatorName() {
        kafkaSource.generateOperatorName("a", "b");
    }

    @Test
    public void getRowTypeInformation() {
        KafkaSourceTableInfo kafkaSourceTableInfo = new KafkaSourceTableInfo();
        kafkaSourceTableInfo.setFieldClasses(new Class[]{String.class});
        kafkaSourceTableInfo.setFields(new String[]{"a"});
        kafkaSource.getRowTypeInformation(kafkaSourceTableInfo);

    }

//    @Test
    public void setStartPosition(){
        FlinkKafkaConsumerBase flinkKafkaConsumerBase = Mockito.mock(FlinkKafkaConsumerBase.class);
//        kafkaSource.setStartPosition("1", "a", flinkKafkaConsumerBase);
    }

    @Test
    public void buildOffsetMap(){
        kafkaSource.buildOffsetMap("{\"12\":\"12\"}", "a");
    }
}
