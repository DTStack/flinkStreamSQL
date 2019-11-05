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

package com.dtstack.flink.sql.source.kafka.metric;

import org.apache.flink.metrics.Gauge;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.IsolationLevel;

/**
 * @author: chuixue
 * @create: 2019-11-05 11:09
 * @description:
 **/
public class KafkaTopicPartitionLagMetric implements Gauge<Long> {

    private SubscriptionState subscriptionState;

    private TopicPartition tp;

    public KafkaTopicPartitionLagMetric(SubscriptionState subscriptionState, TopicPartition tp){
        this.subscriptionState = subscriptionState;
        this.tp = tp;
    }

    @Override
    public Long getValue() {
        return subscriptionState.partitionLag(tp, IsolationLevel.READ_UNCOMMITTED);
    }
}
