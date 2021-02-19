package com.dtstack.flink.sql.sink.kafka;

import com.dtstack.flink.sql.sink.kafka.table.KafkaSinkTableInfo;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.Test;

import java.util.Optional;
import java.util.Properties;
import java.util.stream.IntStream;

import static org.mockito.Mockito.*;

public class KafkaProducerFactoryTest {

    @Test
    public void mockCreateKafkaProducer(){
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

        TypeInformation typeInformation = new TupleTypeInfo(new RowTypeInfo(getTypeInformations(kafkaSinkTableInfo), new String[]{"ss"}));

        Properties properties = getKafkaProperties(kafkaSinkTableInfo);

        FlinkKafkaPartitioner partitioner = mock(FlinkKafkaPartitioner.class);

        KafkaProducerFactory kafkaProducerFactory = new KafkaProducerFactory();
        KafkaProducerFactory kafkaProducerFactorySpy = spy(kafkaProducerFactory);
        kafkaProducerFactorySpy.createKafkaProducer(kafkaSinkTableInfo, typeInformation, properties, Optional.of(partitioner), null);
        verify(kafkaProducerFactorySpy).createKafkaProducer(kafkaSinkTableInfo, typeInformation, properties, Optional.of(partitioner), null);
    }

      protected TypeInformation[] getTypeInformations(KafkaSinkTableInfo kafka11SinkTableInfo) {
        Class<?>[] fieldClasses = kafka11SinkTableInfo.getFieldClasses();
        TypeInformation[] types = IntStream.range(0, fieldClasses.length)
                .mapToObj(i -> TypeInformation.of(fieldClasses[i]))
                .toArray(TypeInformation[]::new);
        return types;
    }

    protected Properties getKafkaProperties(KafkaSinkTableInfo KafkaSinkTableInfo) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaSinkTableInfo.getBootstrapServers());

        for (String key : KafkaSinkTableInfo.getKafkaParamKeys()) {
            props.setProperty(key, KafkaSinkTableInfo.getKafkaParam(key));
        }
        return props;
    }

}
