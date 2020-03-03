package com.dtstack.flink.sql.sink.kafka;


import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.util.Preconditions;

public class CustomerFlinkPartition<T> extends FlinkKafkaPartitioner<T> {
    private static final long serialVersionUID = 1L;
    private int parallelInstanceId;

    public CustomerFlinkPartition() {
    
    }

    public void open(int parallelInstanceId, int parallelInstances) {
        Preconditions.checkArgument(parallelInstanceId >= 0, "Id of this subtask cannot be negative.");
        Preconditions.checkArgument(parallelInstances > 0, "Number of subtasks must be larger than 0.");
        this.parallelInstanceId = parallelInstanceId;
    }

    public int partition(T record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        Preconditions.checkArgument(partitions != null && partitions.length > 0, "Partitions of the target topic is empty.");
        if(key == null){
            return partitions[this.parallelInstanceId % partitions.length];
        }
        return partitions[key.hashCode() % partitions.length];
    }

    public boolean equals(Object o) {
        return this == o || o instanceof CustomerFlinkPartition;
    }

    public int hashCode() {
        return CustomerFlinkPartition.class.hashCode();
    }
}
