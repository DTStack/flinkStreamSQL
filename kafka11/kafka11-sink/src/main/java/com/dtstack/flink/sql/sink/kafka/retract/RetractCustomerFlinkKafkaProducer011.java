/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flink.sql.sink.kafka.retract;

import com.dtstack.flink.sql.metric.MetricConstant;
import com.dtstack.flink.sql.sink.kafka.CustomerJsonRowSerializationSchema;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

import java.util.Optional;
import java.util.Properties;

/**
 * Reason:
 * Date: 2019/4/24
 * Company: www.dtstack.com
 *
 * @author maqi
 */
public class RetractCustomerFlinkKafkaProducer011<Row> extends FlinkKafkaProducer011<Row> {

    private static final long serialVersionUID = -613215751656348171L;
    CustomerJsonRowSerializationSchema schema;

    public RetractCustomerFlinkKafkaProducer011(String topicId, SerializationSchema<Row> serializationSchema, Properties producerConfig, Optional<FlinkKafkaPartitioner<Row>> customPartitioner) {
        //  Too many ongoing snapshots. Increase kafka producers pool size or decrease number of concurrent checkpoints.  默认KafakProductPoolSize必须大于1
        super(topicId, new KeyedSerializationSchemaWrapper(serializationSchema), producerConfig, customPartitioner, FlinkKafkaProducer011.Semantic.EXACTLY_ONCE, 5);
        this.schema = (CustomerJsonRowSerializationSchema) serializationSchema;
    }


    @Override
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);

        RuntimeContext ctx = getRuntimeContext();
        Counter counter = ctx.getMetricGroup().counter(MetricConstant.DT_NUM_RECORDS_OUT);

        schema.setCounter(counter);
    }
}