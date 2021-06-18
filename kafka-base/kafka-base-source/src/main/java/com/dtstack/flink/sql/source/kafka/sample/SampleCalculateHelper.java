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

import java.util.HashMap;
import java.util.Map;

/**
 * Date: 2021/05/25 Company: www.dtstack.com
 *
 * @author tiezhu
 */
public class SampleCalculateHelper {

    /** 根据获取的 begin-offset 和 end-offset 来重新规划 */
    public static Map<KafkaTopicPartition, Long> rebuildAssignedPartitionsWithInitialOffsets(
            OffsetMap offsetMap,
            Long sampleSize,
            Map<KafkaTopicPartition, Long> assignedPartitionsWithInitialOffsets) {

        Map<KafkaTopicPartition, Long> latest = offsetMap.getLatest();
        Map<KafkaTopicPartition, Long> earliest = offsetMap.getEarliest();
        Map<KafkaTopicPartition, Long> resetOffsets = new HashMap<>();

        Map<KafkaTopicPartition, Long> sampleOfEachPartition =
                calculateSampleSizeOfEachPartition(sampleSize, offsetMap);

        for (KafkaTopicPartition partition : assignedPartitionsWithInitialOffsets.keySet()) {
            Long earliestOffset = earliest.get(partition);
            Long latestOffset = latest.get(partition);
            // 判断当前kafka 可消费数是否满足 样本大小sampleSize
            Long sampleOfCurrentPartition = sampleOfEachPartition.get(partition);

            if (offsetMap.getDifference(partition) >= sampleOfCurrentPartition) {
                resetOffsets.put(partition, latestOffset - sampleOfCurrentPartition - 1);
            } else {
                resetOffsets.put(partition, earliestOffset - 1);
            }
        }

        return resetOffsets;
    }

    /**
     * Calculate the sample size.
     *
     * @param sampleSize sample size
     * @param offsetMap offset map
     * @return the map of partition and sample size
     */
    private static Map<KafkaTopicPartition, Long> calculateSampleSizeOfEachPartition(
            Long sampleSize, OffsetMap offsetMap) {
        Map<KafkaTopicPartition, Long> sampleSizeOfPartition = new HashMap<>();

        Map<KafkaTopicPartition, Long> latest = offsetMap.getLatest();

        // 均分的样本数，在样本数小于分区数的情况下，样本数为 0
        long eachSampleSize = sampleSize / latest.keySet().size();
        // 余数，在部分场景下会丢失
        long remainder = sampleSize % latest.keySet().size();

        // 样本数小于分区数
        if (eachSampleSize == 0) {
            for (KafkaTopicPartition partition : latest.keySet()) {
                if (remainder <= 0) {
                    sampleSizeOfPartition.put(partition, 0L);
                } else if (offsetMap.getDifference(partition) > 0) {
                    sampleSizeOfPartition.put(partition, 1L);
                    remainder--;
                } else {
                    sampleSizeOfPartition.put(partition, 0L);
                }
            }
        } else {
            // 样本数大于分区数
            for (KafkaTopicPartition partition : latest.keySet()) {
                if (offsetMap.getDifference(partition) >= eachSampleSize) {
                    sampleSizeOfPartition.put(partition, eachSampleSize);
                } else {
                    sampleSizeOfPartition.put(partition, offsetMap.getDifference(partition));
                }
            }
        }

        return sampleSizeOfPartition;
    }
}
