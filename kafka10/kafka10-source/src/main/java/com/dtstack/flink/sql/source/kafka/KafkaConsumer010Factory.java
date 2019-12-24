package com.dtstack.flink.sql.source.kafka;

import com.dtstack.flink.sql.format.FormatType;
import com.dtstack.flink.sql.source.kafka.table.KafkaSourceTableInfo;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.AvroRowDeserializationSchema;
import org.apache.flink.formats.csv.CsvRowDeserializationSchema;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.types.Row;

import java.util.Properties;
import java.util.regex.Pattern;

/**
 * company: www.dtstack.com
 * author: toutian
 * create: 2019/12/24
 */
public class KafkaConsumer010Factory {

    public static CustomerKafkaConsumer010 createKafkaTableSource(KafkaSourceTableInfo kafkaSourceTableInfo, TypeInformation<Row> typeInformation, Properties props) {
        DeserializationSchema<Row> deserializationSchema = null;
        if (FormatType.JSON.name().equalsIgnoreCase(kafkaSourceTableInfo.getSourceDataType())) {
            deserializationSchema = new JsonRowDeserializationSchema(typeInformation);
        } else if (FormatType.CSV.name().equalsIgnoreCase(kafkaSourceTableInfo.getSourceDataType())) {
            final CsvRowDeserializationSchema.Builder deserSchemaBuilder = new CsvRowDeserializationSchema.Builder(typeInformation);
            deserSchemaBuilder.setFieldDelimiter(kafkaSourceTableInfo.getFieldDelimiter().toCharArray()[0]);
            deserializationSchema = deserSchemaBuilder.build();
        } else if (FormatType.AVRO.name().equalsIgnoreCase(kafkaSourceTableInfo.getSourceDataType())) {
            deserializationSchema = new AvroRowDeserializationSchema(kafkaSourceTableInfo.getSchemaString());
        }

        if (null == deserializationSchema) {
            throw new UnsupportedOperationException("FormatType:" + kafkaSourceTableInfo.getSourceDataType());
        }

        CustomerKafkaConsumer010 kafkaSrc = null;
        if (kafkaSourceTableInfo.getTopicIsPattern()) {
            kafkaSrc = new CustomerKafkaConsumer010(Pattern.compile(kafkaSourceTableInfo.getTopic()), new KafkaDeserializationMetricWrapper(typeInformation, deserializationSchema), props);
        } else {
            kafkaSrc = new CustomerKafkaConsumer010(kafkaSourceTableInfo.getTopic(), new KafkaDeserializationMetricWrapper(typeInformation, deserializationSchema), props);
        }
        return kafkaSrc;
    }

}
