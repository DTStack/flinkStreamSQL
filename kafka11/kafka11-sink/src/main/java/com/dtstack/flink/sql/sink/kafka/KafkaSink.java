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
import com.dtstack.flink.sql.table.AbstractTargetTableInfo;

import java.util.Optional;
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
public class KafkaSink extends AbstractKafkaSink {
    @Override
    public KafkaSink genStreamSink(AbstractTargetTableInfo targetTableInfo) {
        KafkaSinkTableInfo kafka11SinkTableInfo = (KafkaSinkTableInfo) targetTableInfo;

        Properties kafkaProperties = getKafkaProperties(kafka11SinkTableInfo);
        this.tableName = kafka11SinkTableInfo.getName();
        this.updateMode = kafka11SinkTableInfo.getUpdateMode();
        this.topic = kafka11SinkTableInfo.getTopic();
        this.partitioner = Optional.of(new CustomerFlinkPartition<>());
        this.partitionKeys = getPartitionKeys(kafka11SinkTableInfo);
        this.fieldNames = kafka11SinkTableInfo.getFields();
        this.fieldTypes = getTypeInformations(kafka11SinkTableInfo);
        this.schema = buildTableSchema(fieldNames, fieldTypes);
        this.parallelism = kafka11SinkTableInfo.getParallelism();
        this.sinkOperatorName = SINK_OPERATOR_NAME_TPL.replace("${topic}", topic).replace("${table}", tableName);
        this.kafkaProducer = new KafkaProducer011Factory().createKafkaProducer(kafka11SinkTableInfo, getRowTypeInfo(), kafkaProperties, partitioner, partitionKeys);
        return this;
    }
}
