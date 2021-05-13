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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaDeserializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author tiezhu
 * @date 2021/4/26
 * Company dtstack
 */
public class DtKafkaDeserializationSchemaWrapper<T> extends KafkaDeserializationSchemaWrapper<T> {

    private final Map<KafkaTopicPartition, Long> specificEndOffsets;

    private final List<Integer> endPartition = new ArrayList<>();

    public DtKafkaDeserializationSchemaWrapper(DeserializationSchema<T> deserializationSchema,
                                               Map<KafkaTopicPartition, Long> specificEndOffsets) {

        super(deserializationSchema);
        this.specificEndOffsets = specificEndOffsets;
    }

    @Override
    public T deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        KafkaTopicPartition topicPartition = new KafkaTopicPartition(record.topic(), record.partition());
        if (endPartition.contains(record.partition())) {
            return null;
        }
        if (specificEndOffsets != null) {
            Long endOffset = specificEndOffsets.get(topicPartition);
            if (endOffset != null && record.offset() >= endOffset) {
                endPartition.add(record.partition());
                return null;
            }
        }

        return super.deserialize(record);
    }

    public boolean isEndOfStream(T nextElement) {
        boolean isEnd =
                specificEndOffsets != null
                        && endPartition.size() == specificEndOffsets.size();
        return super.isEndOfStream(nextElement) || isEnd;
    }
}
