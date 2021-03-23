package com.dtstack.flink.sql.source.kafka;

import com.dtstack.flink.sql.source.kafka.table.KafkaSourceTableInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AbstractKafkaConsumerFactoryTest {

    private AbstractKafkaConsumerFactory kafkaConsumerFactory;
    private RowTypeInfo typeInformation;

    @Before
    public void init(){
        kafkaConsumerFactory = new AbstractKafkaConsumerFactory() {
            @Override
            protected FlinkKafkaConsumerBase<Row> createKafkaTableSource(KafkaSourceTableInfo kafkaSourceTableInfo, TypeInformation<Row> typeInformation, Properties props) {
                return null;
            }
        };

        String[] fieldNames = new String[1];
        fieldNames[0] = "1";
        TypeInformation[] typeInformations = new TypeInformation[1];
        typeInformations[0] = Types.STRING;
        typeInformation = new RowTypeInfo(typeInformations, fieldNames);
    }

    @Test
    public void createDeserializationMetricWrapper(){
        KafkaSourceTableInfo kafkaSourceTableInfo = mock(KafkaSourceTableInfo.class);
        when(kafkaSourceTableInfo.getSourceDataType()).thenReturn("DT_NEST");
        Calculate calculate = mock(Calculate.class);
        kafkaConsumerFactory.createDeserializationMetricWrapper(kafkaSourceTableInfo, typeInformation, calculate);


        when(kafkaSourceTableInfo.getSourceDataType()).thenReturn("JSON");
        kafkaConsumerFactory.createDeserializationMetricWrapper(kafkaSourceTableInfo, typeInformation, calculate);

        when(kafkaSourceTableInfo.getSourceDataType()).thenReturn("CSV");
        when(kafkaSourceTableInfo.getFieldDelimiter()).thenReturn(",");
        kafkaConsumerFactory.createDeserializationMetricWrapper(kafkaSourceTableInfo, typeInformation, calculate);


        when(kafkaSourceTableInfo.getSourceDataType()).thenReturn("AVRO");
        when(kafkaSourceTableInfo.getSchemaString()).thenReturn("{\"type\":\"record\",\"name\":\"MyResult\",\"fields\":[{\"name\":\"channel\",\"type\":\"string\"}]}");
        kafkaConsumerFactory.createDeserializationMetricWrapper(kafkaSourceTableInfo, typeInformation, calculate);

    }

}
