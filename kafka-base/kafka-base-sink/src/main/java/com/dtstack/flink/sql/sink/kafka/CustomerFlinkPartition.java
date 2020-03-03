package com.dtstack.flink.sql.sink.kafka;


import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;

import org.apache.flink.util.Preconditions;

public class CustomerFlinkPartition<T> extends FlinkFixedPartitioner<T> {
    @Override
    public int partition(T record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        Preconditions.checkArgument(partitions != null && partitions.length > 0, "Partitions of the target topic is empty.");
        if(key == null){
            return super.partition(record, key, value, targetTopic, partitions);
        }
        return partitions[key.hashCode() % partitions.length];
    }
}
