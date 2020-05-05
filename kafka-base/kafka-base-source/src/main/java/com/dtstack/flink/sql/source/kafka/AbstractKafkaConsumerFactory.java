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

package com.dtstack.flink.sql.source.kafka;

import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.AvroRowDeserializationSchema;
import org.apache.flink.formats.csv.CsvRowDeserializationSchema;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.types.Row;

import com.dtstack.flink.MoreSuppliers;
import com.dtstack.flink.formats.protobuf.ProtobufRowDeserializationSchema;
import com.dtstack.flink.formats.protobuf.ProtobufRowFormatFactory;
import com.dtstack.flink.sql.format.DeserializationMetricWrapper;
import com.dtstack.flink.sql.format.FormatType;
import com.dtstack.flink.sql.format.dtnest.DtNestRowDeserializationSchema;
import com.dtstack.flink.sql.source.kafka.table.KafkaSourceTableInfo;
import com.google.protobuf.GeneratedMessageV3;

/**
 *
 * company: www.dtstack.com
 * @author: toutian
 * create: 2019/12/24
 */
public abstract class AbstractKafkaConsumerFactory {

    protected abstract FlinkKafkaConsumerBase<Row> createKafkaTableSource(KafkaSourceTableInfo kafkaSourceTableInfo,
                                                                          TypeInformation<Row> typeInformation,
                                                                          Properties props);

    protected DeserializationMetricWrapper createDeserializationMetricWrapper(KafkaSourceTableInfo kafkaSourceTableInfo,
                                                                              TypeInformation<Row> typeInformation,
                                                                              Calculate calculate) {
        return new KafkaDeserializationMetricWrapper(typeInformation,
                createDeserializationSchema(kafkaSourceTableInfo, typeInformation),
                calculate);
    }

    @SuppressWarnings("unchecked")
    private DeserializationSchema<Row> createDeserializationSchema(KafkaSourceTableInfo kafkaSourceTableInfo, TypeInformation<Row> typeInformation) {
        DeserializationSchema<Row> deserializationSchema = null;
        if (FormatType.DT_NEST.name().equalsIgnoreCase(kafkaSourceTableInfo.getSourceDataType())) {
            deserializationSchema = new DtNestRowDeserializationSchema(typeInformation, kafkaSourceTableInfo.getPhysicalFields(), kafkaSourceTableInfo.getFieldExtraInfoList());

        } else if (FormatType.JSON.name().equalsIgnoreCase(kafkaSourceTableInfo.getSourceDataType())) {

            if (StringUtils.isNotBlank(kafkaSourceTableInfo.getSchemaString())) {
                deserializationSchema = new JsonRowDeserializationSchema(kafkaSourceTableInfo.getSchemaString());
            } else if (typeInformation != null && typeInformation.getArity() != 0) {
                deserializationSchema = new JsonRowDeserializationSchema(typeInformation);
            } else {
                throw new IllegalArgumentException("sourceDataType:" + FormatType.JSON.name() + " must set schemaString（JSON Schema）or TypeInformation<Row>");
            }

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
        } else if (FormatType.PROTOBUF.name().equalsIgnoreCase(kafkaSourceTableInfo.getSourceDataType())) {

            if (StringUtils.isNotBlank(kafkaSourceTableInfo.getDescriptorHttpGetUrl())) {
                byte[] descriptorBytes = ProtobufRowFormatFactory.httpGetDescriptorBytes(kafkaSourceTableInfo.getDescriptorHttpGetUrl());

                deserializationSchema = new ProtobufRowDeserializationSchema(descriptorBytes);
            } else if (StringUtils.isNotBlank(kafkaSourceTableInfo.getMessageClassString())) {

                Class<?> messageClazz = MoreSuppliers.throwing(() -> Class.forName(kafkaSourceTableInfo.getMessageClassString()));

                if (!GeneratedMessageV3.class.isAssignableFrom(messageClazz)) {
                    throw new RuntimeException("message class is not GeneratedMessageV3 type");
                }

                deserializationSchema = new ProtobufRowDeserializationSchema((Class<? extends GeneratedMessageV3>) messageClazz);

            } else {

                throw new IllegalArgumentException("sinkDataType:" + FormatType.PROTOBUF.name() + " must set descriptorHttpGetUrl or messageClassString");
            }
        }

        if (null == deserializationSchema) {
            throw new UnsupportedOperationException("FormatType:" + kafkaSourceTableInfo.getSourceDataType());
        }

        return deserializationSchema;
    }

}
