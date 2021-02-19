package com.dtstack.flink.sql.sink.kafka;

import com.dtstack.flink.sql.sink.kafka.table.KafkaSinkTableInfo;
import com.dtstack.flink.sql.table.AbstractTargetTableInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.*;

public class AbstractKafkaSinkTest {
    private  AbstractKafkaSink kafkaSink;

    @Before
    public void init(){
        kafkaSink = new AbstractKafkaSink() {
            @Override
            public Object genStreamSink(AbstractTargetTableInfo targetTableInfo) {
                return null;
            }
        };
    }

//    @Test
    public void testKafkaSink(){
        KafkaSinkTableInfo kafkaSinkTableInfo = new KafkaSinkTableInfo();
        kafkaSinkTableInfo.setBootstrapServers("localhost:9092");
        kafkaSinkTableInfo.setPartitionKeys("a,b");
        kafkaSink.getKafkaProperties(kafkaSinkTableInfo);
        kafkaSink.getPartitionKeys(kafkaSinkTableInfo);


    }

//    @Test
    public void getTypeInformations(){
        KafkaSinkTableInfo kafkaSinkTableInfo = new KafkaSinkTableInfo();
        Class[] fieldClass = new Class[1];
        fieldClass[0] = String.class;
        kafkaSinkTableInfo.setFieldClasses(fieldClass);
        kafkaSink.getTypeInformations(kafkaSinkTableInfo);
    }

    // @Test
    public void buildTableSchema(){
        String[] fieldNames = new String[1];
        fieldNames[0] = "1";
        TypeInformation[] typeInformations = new TypeInformation[1];
        typeInformations[0] = mock(TypeInformation.class);
        kafkaSink.buildTableSchema(fieldNames, typeInformations);
    }

    // @Test
    public void emitDataStream(){
        String[] fieldNames = new String[1];
        fieldNames[0] = "1";
        TypeInformation[] typeInformations = new TypeInformation[1];
        typeInformations[0] = mock(TypeInformation.class);
        kafkaSink.configure(fieldNames, typeInformations);
        kafkaSink.getFieldNames();
        DataStream dataStream = mock(DataStream.class);
        DataStreamSink dataStreamSink = mock(DataStreamSink.class);
        SingleOutputStreamOperator singleOutputStreamOperator = mock(SingleOutputStreamOperator.class);
        when(dataStream.map(any())).thenReturn(singleOutputStreamOperator);
        when(singleOutputStreamOperator.setParallelism(anyInt())).thenReturn(singleOutputStreamOperator);
        when(singleOutputStreamOperator.addSink(anyObject())).thenReturn(dataStreamSink);
        when(dataStreamSink.name(anyString())).thenReturn(dataStreamSink);
        kafkaSink.emitDataStream(dataStream);

    }


}
