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

import com.dtstack.flink.sql.source.kafka.deserialization.DtKafkaDeserializationSchemaWrapper;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @auther tiezhu
 * @date 2021/4/27 4:33 下午
 */
public class DtKafkaDeserializationSchemaWrapperTest {
    public Map<KafkaTopicPartition, Long> specificEndOffsets;

    private DtKafkaDeserializationSchemaWrapper<String> kafkaDeserializationSchemaWrapper;

    @Before
    public void setUp() {
        String topic = "test";
        Map<String, Object> valueMap = new HashMap<>();

        valueMap.put("1", 10);

        specificEndOffsets = valueMap
                .entrySet()
                .stream()
                .collect(Collectors.toMap(
                        (Map.Entry<String, Object> entry)
                                -> new KafkaTopicPartition(topic, Integer.parseInt(entry.getKey())),
                        (Map.Entry<String, Object> entry)
                                -> Long.valueOf(entry.getValue().toString()))
                );

        kafkaDeserializationSchemaWrapper = new DtKafkaDeserializationSchemaWrapper<>(new SimpleStringSchema());
    }

    @Test
    public void testIsEndOfStream() {
        String topic = "test";

        Assert.assertFalse(kafkaDeserializationSchemaWrapper.isEndOfStream(topic));
    }

    @Test
    public void testDeserialize() throws Exception {
        String str = "test";
        ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
                "test",
                1,
                11,
                str.getBytes(StandardCharsets.UTF_8),
                str.getBytes(StandardCharsets.UTF_8)
        );

        String deserialize = kafkaDeserializationSchemaWrapper.deserialize(record);

        Assert.assertNull(deserialize);
    }
}
