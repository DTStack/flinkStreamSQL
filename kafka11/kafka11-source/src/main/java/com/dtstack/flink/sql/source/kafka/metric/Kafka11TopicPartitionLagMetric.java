package com.dtstack.flink.sql.source.kafka.metric;

import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.requests.IsolationLevel;

/**
 * Reason:
 * Date: 2018/10/24
 * Company: www.dtstack.com
 * @author xuchao
 */

public class Kafka11TopicPartitionLagMetric extends KafkaTopicPartitionLagMetric {

    public Kafka11TopicPartitionLagMetric(SubscriptionState subscriptionState, TopicPartition tp) {
        super(subscriptionState, tp);
    }

    @Override
    public Long getValue() {
        return subscriptionState.partitionLag(tp, IsolationLevel.READ_UNCOMMITTED);
    }
}
