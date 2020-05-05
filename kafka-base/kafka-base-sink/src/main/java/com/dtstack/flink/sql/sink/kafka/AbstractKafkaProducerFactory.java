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

import java.util.Optional;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.AvroRowSerializationSchema;
import org.apache.flink.formats.csv.CsvRowSerializationSchema;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.types.Row;

import com.dtstack.flink.MoreSuppliers;
import com.dtstack.flink.formats.protobuf.ProtobufRowFormatFactory;
import com.dtstack.flink.formats.protobuf.ProtobufRowSerializationSchema;
import com.dtstack.flink.sql.format.FormatType;
import com.dtstack.flink.sql.format.SerializationMetricWrapper;
import com.dtstack.flink.sql.sink.kafka.table.KafkaSinkTableInfo;
import com.google.protobuf.GeneratedMessageV3;

/**
 * 抽象的kafka producer 的工厂类
 * 包括序统一的序列化工具的构造
 * company: www.dtstack.com
 * @author: toutian
 * create: 2019/12/26
 */
public abstract class AbstractKafkaProducerFactory {

    /**
     *  获取具体的KafkaProducer
     * eg create KafkaProducer010
     * @param kafkaSinkTableInfo
     * @param typeInformation
     * @param properties
     * @param partitioner
     * @return
     */
    public abstract RichSinkFunction<Row> createKafkaProducer(KafkaSinkTableInfo kafkaSinkTableInfo, TypeInformation<Row> typeInformation, Properties properties, Optional<FlinkKafkaPartitioner<Row>> partitioner, String[] partitionKeys);

    protected SerializationMetricWrapper createSerializationMetricWrapper(KafkaSinkTableInfo kafkaSinkTableInfo, TypeInformation<Row> typeInformation) {
        return new SerializationMetricWrapper(createSerializationSchema(kafkaSinkTableInfo, typeInformation));
    }

    @SuppressWarnings("unchecked")
    private SerializationSchema<Row> createSerializationSchema(KafkaSinkTableInfo kafkaSinkTableInfo, TypeInformation<Row> typeInformation) {
        SerializationSchema<Row> serializationSchema = null;
        if (FormatType.JSON.name().equalsIgnoreCase(kafkaSinkTableInfo.getSinkDataType())) {

            if (StringUtils.isNotBlank(kafkaSinkTableInfo.getSchemaString())) {
                serializationSchema = new JsonRowSerializationSchema(kafkaSinkTableInfo.getSchemaString());
            } else if (typeInformation != null && typeInformation.getArity() != 0) {
                serializationSchema = new JsonRowSerializationSchema(typeInformation);
            } else {
                throw new IllegalArgumentException("sinkDataType:" + FormatType.JSON.name() + " must set schemaString（JSON Schema）or TypeInformation<Row>");
            }

        } else if (FormatType.CSV.name().equalsIgnoreCase(kafkaSinkTableInfo.getSinkDataType())) {

            if (StringUtils.isBlank(kafkaSinkTableInfo.getFieldDelimiter())) {
                throw new IllegalArgumentException("sinkDataType:" + FormatType.CSV.name() + " must set fieldDelimiter");
            }

            final CsvRowSerializationSchema.Builder serSchemaBuilder = new CsvRowSerializationSchema.Builder(typeInformation);
            serSchemaBuilder.setFieldDelimiter(kafkaSinkTableInfo.getFieldDelimiter().toCharArray()[0]);
            serializationSchema = serSchemaBuilder.build();

        } else if (FormatType.AVRO.name().equalsIgnoreCase(kafkaSinkTableInfo.getSinkDataType())) {

            if (StringUtils.isBlank(kafkaSinkTableInfo.getSchemaString())) {
                throw new IllegalArgumentException("sinkDataType:" + FormatType.AVRO.name() + " must set schemaString");
            }

            serializationSchema = new AvroRowSerializationSchema(kafkaSinkTableInfo.getSchemaString());

        } else if (FormatType.PROTOBUF.name().equalsIgnoreCase(kafkaSinkTableInfo.getSinkDataType())) {

            if (StringUtils.isNotBlank(kafkaSinkTableInfo.getDescriptorHttpGetUrl())) {
                byte[] descriptorBytes = ProtobufRowFormatFactory.httpGetDescriptorBytes(kafkaSinkTableInfo.getDescriptorHttpGetUrl());

                serializationSchema = new ProtobufRowSerializationSchema(descriptorBytes);
            } else if (StringUtils.isNotBlank(kafkaSinkTableInfo.getMessageClassString())) {

                Class<?> messageClazz = MoreSuppliers.throwing(() -> Class.forName(kafkaSinkTableInfo.getMessageClassString()));

                if (!GeneratedMessageV3.class.isAssignableFrom(messageClazz)) {
                    throw new RuntimeException("message class is not GeneratedMessageV3 type");
                }

                serializationSchema = new ProtobufRowSerializationSchema((Class<? extends GeneratedMessageV3>) messageClazz);

            } else {

                throw new IllegalArgumentException("sinkDataType:" + FormatType.PROTOBUF.name() + " must set descriptorHttpGetUrl or messageClassString");
            }
        }

        if (null == serializationSchema) {
            throw new UnsupportedOperationException("FormatType:" + kafkaSinkTableInfo.getSinkDataType());
        }

        return serializationSchema;
    }

}
