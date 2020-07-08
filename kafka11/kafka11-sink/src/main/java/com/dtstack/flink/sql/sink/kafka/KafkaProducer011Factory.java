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

import com.dtstack.flink.sql.sink.kafka.table.KafkaSinkTableInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.runtime.types.CRow;

import java.util.Optional;
import java.util.Properties;

/**
 * company: www.dtstack.com
 *
 * @author: toutian
 * create: 2019/12/24
 */
public class KafkaProducer011Factory extends AbstractKafkaProducerFactory {

    @Override
    public RichSinkFunction<CRow> createKafkaProducer(KafkaSinkTableInfo kafkaSinkTableInfo, TypeInformation<CRow> typeInformation, Properties properties, Optional<FlinkKafkaPartitioner<CRow>> partitioner,
            String[] partitionKeys) {
        return new KafkaProducer011(kafkaSinkTableInfo.getTopic(), createSerializationMetricWrapper(kafkaSinkTableInfo, typeInformation), properties, partitioner, partitionKeys);
    }
}
