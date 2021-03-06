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

import com.dtstack.flink.sql.dirtyManager.manager.DirtyDataManager;
import com.dtstack.flink.sql.format.DeserializationMetricWrapper;
import com.dtstack.flink.sql.source.kafka.deserialization.DeserializationSchemaFactory;
import com.dtstack.flink.sql.source.kafka.deserialization.KafkaDeserializationMetricWrapper;
import com.dtstack.flink.sql.source.kafka.table.KafkaSourceTableInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.types.Row;

import java.util.Properties;

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
                DeserializationSchemaFactory.createDeserializationSchema(kafkaSourceTableInfo, typeInformation),
                calculate,
                DirtyDataManager.newInstance(kafkaSourceTableInfo.getDirtyProperties()));
    }
}
