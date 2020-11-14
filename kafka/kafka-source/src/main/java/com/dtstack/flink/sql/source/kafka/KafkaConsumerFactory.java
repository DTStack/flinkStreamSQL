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

import com.dtstack.flink.sql.format.DeserializationMetricWrapper;
import com.dtstack.flink.sql.source.kafka.table.KafkaSourceTableInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * company: www.dtstack.com
 *
 * @author: toutian
 * create: 2019/12/24
 */
public class KafkaConsumerFactory extends AbstractKafkaConsumerFactory implements Serializable{
    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerFactory.class);

    @Override
    public FlinkKafkaConsumerBase<Row> createKafkaTableSource(KafkaSourceTableInfo kafkaSourceTableInfo, TypeInformation<Row> typeInformation, Properties props) {
        KafkaConsumer kafkaSrc;
        if (kafkaSourceTableInfo.getTopicIsPattern()) {
            DeserializationMetricWrapper deserMetricWrapper = createDeserializationMetricWrapper(kafkaSourceTableInfo, typeInformation, (Calculate & Serializable) (subscriptionState, tp) -> {
                try {
                    return partitionLag(subscriptionState ,tp);
                } catch (Exception e) {
                    LOG.error(e.toString());
                }
                return null;
            });
            kafkaSrc = new KafkaConsumer(Pattern.compile(kafkaSourceTableInfo.getTopic()), deserMetricWrapper, props);
        } else {
            DeserializationMetricWrapper deserMetricWrapper = createDeserializationMetricWrapper(kafkaSourceTableInfo, typeInformation, (Calculate & Serializable) (subscriptionState, tp) -> {
                try {
                    return partitionLag(subscriptionState ,tp);
                } catch (Exception e) {
                    LOG.error(e.toString());
                }
                return null;
            });
            kafkaSrc = new KafkaConsumer(kafkaSourceTableInfo.getTopic(), deserMetricWrapper, props);
        }
        return kafkaSrc;
    }

    /**
     * 获取kafka的lag
     * @param subscriptionState
     * @param topicPartition
     * @return
     * @throws Exception
     */
    private Long partitionLag(SubscriptionState subscriptionState, TopicPartition topicPartition) throws Exception {
        Method assignedState = subscriptionState.getClass().getDeclaredMethod("assignedState", TopicPartition.class);
        assignedState.setAccessible(true);
        Object subscriptionStateInvoke = assignedState.invoke(subscriptionState, topicPartition);

        Field highWatermarkField = subscriptionStateInvoke.getClass().getDeclaredField("highWatermark");
        highWatermarkField.setAccessible(true);
        Long highWatermark = (Long) highWatermarkField.get(subscriptionStateInvoke);

        Field positionField = subscriptionStateInvoke.getClass().getDeclaredField("position");
        positionField.setAccessible(true);
        SubscriptionState.FetchPosition fetchPosition = (SubscriptionState.FetchPosition) positionField.get(subscriptionStateInvoke);
        long offset = fetchPosition.offset;
        return highWatermark - offset;
    }
}
