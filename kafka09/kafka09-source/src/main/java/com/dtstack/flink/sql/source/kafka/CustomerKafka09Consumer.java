package com.dtstack.flink.sql.source.kafka;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * Reason:
 * Date: 2018/10/12
 * Company: www.dtstack.com
 * @author xuchao
 */

public class CustomerKafka09Consumer extends FlinkKafkaConsumer09<Row> {

    private CustomerJsonDeserialization customerJsonDeserialization;

    public CustomerKafka09Consumer(String topic, CustomerJsonDeserialization valueDeserializer, Properties props) {
        super(topic, valueDeserializer, props);
        this.customerJsonDeserialization = valueDeserializer;
    }

    @Override
    public void run(SourceContext<Row> sourceContext) throws Exception {
        customerJsonDeserialization.setRuntimeContext(getRuntimeContext());
        customerJsonDeserialization.initMetric();
        super.run(sourceContext);
    }
}
