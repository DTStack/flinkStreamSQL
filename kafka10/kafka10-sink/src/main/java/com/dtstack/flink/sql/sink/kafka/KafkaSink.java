/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flink.sql.sink.kafka;

import com.dtstack.flink.sql.sink.IStreamSinkGener;
import com.dtstack.flink.sql.sink.kafka.table.KafkaSinkTableInfo;
import com.dtstack.flink.sql.table.TargetTableInfo;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.Kafka010TableSink;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSink;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.TableSchemaBuilder;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import java.util.Optional;
import java.util.Properties;

public class KafkaSink implements AppendStreamTableSink<Row>, IStreamSinkGener<KafkaSink> {

    protected String[] fieldNames;

    protected TypeInformation<?>[] fieldTypes;

    /** The schema of the table. */
    private TableSchema schema;

    /** The Kafka topic to write to. */
    protected String topic;

    /** Properties for the Kafka producer. */
    protected Properties properties;

    /** Serialization schema for encoding records to Kafka. */
    protected SerializationSchema serializationSchema;

    /** Partitioner to select Kafka partition for each item. */
    protected Optional<FlinkKafkaPartitioner<Row>> partitioner;

    @Override
    public KafkaSink genStreamSink(TargetTableInfo targetTableInfo) {
        KafkaSinkTableInfo kafka010SinkTableInfo = (KafkaSinkTableInfo) targetTableInfo;
        this.topic = kafka010SinkTableInfo.getKafkaParam("topic");

        Properties props = new Properties();
        for (String key:kafka010SinkTableInfo.getKafkaParamKeys()) {
            props.setProperty(key, kafka010SinkTableInfo.getKafkaParam(key));
        }
        this.properties = props;
        this.partitioner = Optional.of(new FlinkFixedPartitioner<>());
        this.fieldNames = kafka010SinkTableInfo.getFields();
        TypeInformation[] types = new TypeInformation[kafka010SinkTableInfo.getFields().length];
        for(int i = 0; i< kafka010SinkTableInfo.getFieldClasses().length; i++){
            types[i] = TypeInformation.of(kafka010SinkTableInfo.getFieldClasses()[i]);
        }
        this.fieldTypes = types;

        TableSchemaBuilder schemaBuilder = TableSchema.builder();
        for (int i=0;i<fieldNames.length;i++){
            schemaBuilder.field(fieldNames[i], fieldTypes[i]);
        }
        this.schema = schemaBuilder.build();

        //this.serializationSchema = Optional.of(JsonRowSerializationSchema.class);
        if ("json".equalsIgnoreCase(kafka010SinkTableInfo.getSourceDataType())) {
            this.serializationSchema = new JsonRowSerializationSchema(getOutputType());
        } else if ("csv".equalsIgnoreCase(kafka010SinkTableInfo.getSourceDataType())){
            this.serializationSchema = new TypeInformationSerializationSchema(TypeInformation.of(Row.class),
                    new CustomerCsvSerialization(kafka010SinkTableInfo.getFieldDelimiter(),fieldTypes));
        }
        return this;
    }

    @Override
    public void emitDataStream(DataStream<Row> dataStream) {
        KafkaTableSink kafkaTableSink = new Kafka010TableSink(
                schema,
                topic,
                properties,
                partitioner,
                serializationSchema
        );

        kafkaTableSink.emitDataStream(dataStream);
    }

    @Override
    public TypeInformation<Row> getOutputType() {
        return new RowTypeInfo(fieldTypes, fieldNames);
    }

    @Override
    public String[] getFieldNames() {
        return fieldNames;
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return fieldTypes;
    }

    @Override
    public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        return this;
    }

}
