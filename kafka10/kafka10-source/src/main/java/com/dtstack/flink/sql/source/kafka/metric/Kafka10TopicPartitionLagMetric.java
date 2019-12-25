package com.dtstack.flink.sql.source.kafka.metric;

import org.apache.flink.metrics.Gauge;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.TopicPartition;

/**
 * Reason:
 * Date: 2018/10/24
 * Company: www.dtstack.com
 * @author xuchao
 */

public class KafkaTopicPartitionLagMetric implements Gauge<Long> {

    private SubscriptionState subscriptionState;

    private TopicPartition tp;

    public KafkaTopicPartitionLagMetric(SubscriptionState subscriptionState, TopicPartition tp){
        this.subscriptionState = subscriptionState;
        this.tp = tp;
    }

    @Override
    public Long getValue() {
        return subscriptionState.partitionLag(tp);
    }
}
