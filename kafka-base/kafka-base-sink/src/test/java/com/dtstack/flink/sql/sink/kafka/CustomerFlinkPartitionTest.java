package com.dtstack.flink.sql.sink.kafka;

import com.google.common.collect.Lists;
import org.junit.Test;

public class CustomerFlinkPartitionTest {

    @Test
    public void testCustomerFlinkPartition(){
        CustomerFlinkPartition customerFlinkPartition = new CustomerFlinkPartition();
        customerFlinkPartition.open(1, 1);
        int[] partition = new int[1];
        partition[0] = 1;
        customerFlinkPartition.partition(null, "key".getBytes(), "value".getBytes(), "topic", partition);
        customerFlinkPartition.partition(null, "key".getBytes(), "value".getBytes(), "topic", partition);
        customerFlinkPartition.hashCode();
        customerFlinkPartition.equals(customerFlinkPartition);

    }
}
