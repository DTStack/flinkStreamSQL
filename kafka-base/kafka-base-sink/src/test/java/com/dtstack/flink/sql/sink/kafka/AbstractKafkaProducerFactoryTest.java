package com.dtstack.flink.sql.sink.kafka;

import com.dtstack.flink.sql.sink.kafka.serialization.CsvTupleSerializationSchema;
import com.dtstack.flink.sql.sink.kafka.table.KafkaSinkTableInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Optional;
import java.util.Properties;

@RunWith(PowerMockRunner.class)
@PrepareForTest({CsvTupleSerializationSchema.class, CsvTupleSerializationSchema.Builder.class})
public class AbstractKafkaProducerFactoryTest {
    private AbstractKafkaProducerFactory kafkaProducerFactory;
    private KafkaSinkTableInfo kafkaSinkTableInfo;
    private TypeInformation<Tuple2<Boolean, Row>> typeInformation;

    @Before
    public void init() {
        kafkaProducerFactory = new AbstractKafkaProducerFactory() {
            @Override
            public RichSinkFunction<Tuple2<Boolean, Row>> createKafkaProducer(
                    KafkaSinkTableInfo kafkaSinkTableInfo
                    , TypeInformation<Tuple2<Boolean, Row>> typeInformation
                    , Properties properties
                    , Optional<FlinkKafkaPartitioner<Tuple2<Boolean, Row>>> partitioner
                    , String[] partitionKeys) {
                return null;
            }
        };
        kafkaSinkTableInfo = new KafkaSinkTableInfo();
        String[] fieldNames = new String[1];
        fieldNames[0] = "1";
        TypeInformation[] typeInformation = new TypeInformation[1];
        typeInformation[0] = Types.STRING;
        this.typeInformation = new TupleTypeInfo<>(org.apache.flink.table.api.Types.BOOLEAN(), new RowTypeInfo(typeInformation, fieldNames));
    }

    @Test
    public void createSerializationSchemaWithJson() {
        kafkaSinkTableInfo.setSinkDataType("json");
        kafkaSinkTableInfo.setUpdateMode("append");
        try {
            kafkaProducerFactory.createSerializationMetricWrapper(kafkaSinkTableInfo, typeInformation);
        } catch (Exception ignored) {
        }
        kafkaProducerFactory.createSerializationMetricWrapper(kafkaSinkTableInfo, typeInformation);
        kafkaSinkTableInfo.setSchemaString("{\"name\":\"channel\",\"type\":\"string\"}");
        kafkaProducerFactory.createSerializationMetricWrapper(kafkaSinkTableInfo, typeInformation);
    }

    @Test
    public void createSerializationSchemaWithCsv() {
        kafkaSinkTableInfo.setSinkDataType("csv");
        try {
            kafkaProducerFactory.createSerializationMetricWrapper(kafkaSinkTableInfo, typeInformation);
        } catch (Exception ignored) {
        }
        kafkaSinkTableInfo.setFieldDelimiter(",");
        kafkaSinkTableInfo.setUpdateMode("append");
        kafkaProducerFactory.createSerializationMetricWrapper(kafkaSinkTableInfo, typeInformation);
    }

    @Test
    public void createSerializationSchemaWithAvro() {
        kafkaSinkTableInfo.setSinkDataType("avro");
        try {
            kafkaProducerFactory.createSerializationMetricWrapper(kafkaSinkTableInfo, typeInformation);
        } catch (Exception ignored) {

        }
        kafkaSinkTableInfo.setUpdateMode("append");
        kafkaSinkTableInfo.setSchemaString("{\"name\":\"channel\",\"type\":\"string\"}");
        kafkaProducerFactory.createSerializationMetricWrapper(kafkaSinkTableInfo, typeInformation);
    }
}
