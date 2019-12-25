package com.dtstack.flink.sql.source.kafka;

import com.dtstack.flink.sql.format.dtnest.DtNestRowDeserializationSchema;
import com.dtstack.flink.sql.format.FormatType;
import com.dtstack.flink.sql.source.kafka.table.KafkaSourceTableInfo;
import org.apache.commons.lang3.StringUtils;
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

    public static KafkaConsumer010 createKafkaTableSource(KafkaSourceTableInfo kafkaSourceTableInfo, TypeInformation<Row> typeInformation, Properties props) {
        DeserializationSchema<Row> deserializationSchema = null;
        if (FormatType.DT_NEST.name().equalsIgnoreCase(kafkaSourceTableInfo.getSourceDataType())) {
            deserializationSchema = new DtNestRowDeserializationSchema(typeInformation, kafkaSourceTableInfo.getPhysicalFields(), kafkaSourceTableInfo.getFieldExtraInfoList());
        } else if (FormatType.JSON.name().equalsIgnoreCase(kafkaSourceTableInfo.getSourceDataType())) {
            if (StringUtils.isBlank(kafkaSourceTableInfo.getSchemaString())) {
                throw new IllegalArgumentException("sourceDataType:" + FormatType.JSON.name() + " must set schemaString（JSON Schema）");
            }
            deserializationSchema = new JsonRowDeserializationSchema(kafkaSourceTableInfo.getSchemaString());
        } else if (FormatType.CSV.name().equalsIgnoreCase(kafkaSourceTableInfo.getSourceDataType())) {
            if (StringUtils.isBlank(kafkaSourceTableInfo.getFieldDelimiter())) {
                throw new IllegalArgumentException("sourceDataType:" + FormatType.CSV.name() + " must set fieldDelimiter");
            }
            final CsvRowDeserializationSchema.Builder deserSchemaBuilder = new CsvRowDeserializationSchema.Builder(typeInformation);
            deserSchemaBuilder.setFieldDelimiter(kafkaSourceTableInfo.getFieldDelimiter().toCharArray()[0]);
            deserializationSchema = deserSchemaBuilder.build();
        } else if (FormatType.AVRO.name().equalsIgnoreCase(kafkaSourceTableInfo.getSourceDataType())) {
            if (StringUtils.isBlank(kafkaSourceTableInfo.getSchemaString())) {
                throw new IllegalArgumentException("sourceDataType:" + FormatType.AVRO.name() + " must set schemaString");
            }
            deserializationSchema = new AvroRowDeserializationSchema(kafkaSourceTableInfo.getSchemaString());
        }

        if (null == deserializationSchema) {
            throw new UnsupportedOperationException("FormatType:" + kafkaSourceTableInfo.getSourceDataType());
        }

        KafkaConsumer010 kafkaSrc = null;
        if (kafkaSourceTableInfo.getTopicIsPattern()) {
            kafkaSrc = new KafkaConsumer010(Pattern.compile(kafkaSourceTableInfo.getTopic()), new KafkaDeserializationMetricWrapper(typeInformation, deserializationSchema), props);
        } else {
            kafkaSrc = new KafkaConsumer010(kafkaSourceTableInfo.getTopic(), new KafkaDeserializationMetricWrapper(typeInformation, deserializationSchema), props);
        }
        return kafkaSrc;
    }

}
