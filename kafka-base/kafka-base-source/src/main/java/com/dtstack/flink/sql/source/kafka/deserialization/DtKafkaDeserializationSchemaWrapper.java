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

package com.dtstack.flink.sql.source.kafka.deserialization;

import com.dtstack.flink.sql.source.kafka.sample.OffsetMap;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaDeserializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author tiezhu
 * @date 2021/4/26
 * Company dtstack
 */
public class DtKafkaDeserializationSchemaWrapper<T> extends KafkaDeserializationSchemaWrapper<T> {

    private final List<Integer> endPartition = new ArrayList<>();

    private Map<KafkaTopicPartition, Long> specificEndOffsets;

    public DtKafkaDeserializationSchemaWrapper(DeserializationSchema<T> deserializationSchema) {
        super(deserializationSchema);
    }

    public void setSpecificEndOffsets(OffsetMap offsetMap) {
        Map<KafkaTopicPartition, Long> latest = offsetMap.getLatest();
        Map<KafkaTopicPartition, Long> earliest = offsetMap.getEarliest();

        this.specificEndOffsets = new HashMap<>(latest);

        // 除去没有数据的分区，避免任务一直等待分区数据
        latest.keySet().forEach(
                partition -> {
                    if (latest.get(partition).equals(earliest.get(partition))) {
                        specificEndOffsets.remove(partition);
                    }
                }
        );
    }

    @Override
    public T deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        KafkaTopicPartition topicPartition = new KafkaTopicPartition(record.topic(), record.partition());
        if (endPartition.contains(record.partition())) {
            return null;
        }
        if (specificEndOffsets != null) {
            Long endOffset = specificEndOffsets.get(topicPartition);
            if (endOffset != null && record.offset() >= endOffset - 1) {
                endPartition.add(record.partition());
                return super.deserialize(record);
            }
        }

        return super.deserialize(record);
    }

    public boolean isEndOfStream(T nextElement) {
        boolean isEnd =
                specificEndOffsets != null
                        && !specificEndOffsets.isEmpty()
                        && endPartition.size() == specificEndOffsets.size();
        return super.isEndOfStream(nextElement) || isEnd;
    }
}
