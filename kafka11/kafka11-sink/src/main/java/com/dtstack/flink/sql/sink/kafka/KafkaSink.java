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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSink;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;
import java.util.Properties;

/**
 * kafka result table
 * Date: 2018/12/18
 * Company: www.dtstack.com
 *
 * @author DocLi
 *
 * @modifyer maqi
 *
 */
public class KafkaSink  implements AppendStreamTableSink<Row>, IStreamSinkGener<KafkaSink> {

    protected String[] fieldNames;

    protected TypeInformation<?>[] fieldTypes;

    protected String topic;

    protected Properties properties;

    /** Serialization schema for encoding records to Kafka. */
    protected SerializationSchema serializationSchema;

    @Override
    public KafkaSink genStreamSink(TargetTableInfo targetTableInfo) {
        KafkaSinkTableInfo kafka11SinkTableInfo = (KafkaSinkTableInfo) targetTableInfo;
        this.topic = kafka11SinkTableInfo.getTopic();
        this.fieldNames = kafka11SinkTableInfo.getFields();
        TypeInformation[] types = new TypeInformation[kafka11SinkTableInfo.getFields().length];
        for (int i = 0; i < kafka11SinkTableInfo.getFieldClasses().length; i++) {
            types[i] = TypeInformation.of(kafka11SinkTableInfo.getFieldClasses()[i]);
        }
        this.fieldTypes = types;

        properties = new Properties();
        for (String key : kafka11SinkTableInfo.getKafkaParamKeys()) {
            properties.setProperty(key, kafka11SinkTableInfo.getKafkaParam(key));
        }
        properties.setProperty("bootstrap.servers", kafka11SinkTableInfo.getBootstrapServers());
        this.serializationSchema = new JsonRowSerializationSchema(getOutputType());
        return this;
    }

    @Override
    public void emitDataStream(DataStream<Row> dataStream) {
        KafkaTableSink kafkaTableSink = new CustomerKafka11JsonTableSink(
                topic,
                properties,
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
