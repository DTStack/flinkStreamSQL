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

package com.dtstack.flink.sql.source.kafka.sample;

import com.dtstack.flink.sql.source.kafka.throwable.KafkaSamplingUnavailableException;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.Map;
import java.util.Properties;

/**
 * @author tiezhu
 * @since 2021/6/15 星期二
 */
public interface OffsetFetcher {

    OffsetMap fetchOffset(KafkaConsumer<?, ?> consumer, String topic);

    default OffsetMap seekOffset(Properties props, String topic) {

        setByteDeserializer(props);

        try (KafkaConsumer<?, ?> consumer = new KafkaConsumer<>(props)) {
            OffsetMap offsetMap = fetchOffset(consumer, topic);

            judgeKafkaSampleIsAvailable(offsetMap, topic);

            return offsetMap;
        }
    }

    /**
     * Judge whether there is data for consumption in each partition in Kafka. If there is no data
     * consumption in all partitions, then this sampling task is not available
     *
     * @param offsetMap offset map
     */
    default void judgeKafkaSampleIsAvailable(OffsetMap offsetMap, String topic) {
        boolean kafkaSampleIsAvailable = false;
        Map<KafkaTopicPartition, Long> latest = offsetMap.getLatest();
        Map<KafkaTopicPartition, Long> earliest = offsetMap.getEarliest();

        for (KafkaTopicPartition partition : latest.keySet()) {
            Long earliestOffset = earliest.get(partition);
            Long latestOffset = latest.get(partition);

            if (!latestOffset.equals(earliestOffset)) {
                kafkaSampleIsAvailable = true;
            }
        }

        if (!kafkaSampleIsAvailable) {
            throw new KafkaSamplingUnavailableException(
                    String.format(
                            "Kafka sampling of [%s] is unavailable because there is no data in all partitions",
                            topic));
        }
    }

    /**
     * Makes sure that the ByteArrayDeserializer is registered in the Kafka properties.
     *
     * @param props The Kafka properties to register the serializer in.
     */
    default void setByteDeserializer(Properties props) {

        final String deSerName = ByteArrayDeserializer.class.getName();

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deSerName);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deSerName);
    }
}
