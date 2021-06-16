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

import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * @author tiezhu
 * @since 2021/6/16 星期三
 */
public class OffsetMap implements Serializable {

    private static final long serialVersionUID = 1L;

    /** latest offset of kafka */
    private Map<KafkaTopicPartition, Long> latest = new HashMap<>();

    /** earliest offset of kafka */
    private Map<KafkaTopicPartition, Long> earliest = new HashMap<>();

    public void setLatest(Map<KafkaTopicPartition, Long> latest) {
        this.latest = latest;
    }

    public void setEarliest(Map<KafkaTopicPartition, Long> earliest) {
        this.earliest = earliest;
    }

    public Long getEarliestOffset(KafkaTopicPartition partition) {
        return earliest.get(partition);
    }

    public Long getLatestOffset(KafkaTopicPartition partition) {
        return latest.get(partition);
    }

    public void setLatest(KafkaTopicPartition partition, Long offset) {
        latest.put(partition, offset);
    }

    public void setEarliest(KafkaTopicPartition partition, Long offset) {
        earliest.put(partition, offset);
    }

    public Map<KafkaTopicPartition, Long> getLatest() {
        return latest;
    }

    public Map<KafkaTopicPartition, Long> getEarliest() {
        return earliest;
    }

    public Long getDifference(KafkaTopicPartition partition) {
        return getLatestOffset(partition) - getEarliestOffset(partition);
    }
}
