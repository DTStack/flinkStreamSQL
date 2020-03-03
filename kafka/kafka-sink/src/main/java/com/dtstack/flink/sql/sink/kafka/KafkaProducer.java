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
package com.dtstack.flink.sql.sink.kafka;

import com.dtstack.flink.sql.format.SerializationMetricWrapper;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.types.Row;

import java.util.Optional;
import java.util.Properties;

/**
 * Reason:
 * Date: 2019/4/24
 * Company: www.dtstack.com
 *
 * @author maqi
 */
public class KafkaProducer extends FlinkKafkaProducer<Row> {

    private SerializationMetricWrapper serializationMetricWrapper;

    public KafkaProducer(String topicId, SerializationSchema<Row> serializationSchema, Properties producerConfig, Optional<FlinkKafkaPartitioner<Row>> customPartitioner, String[] parititonKeys) {
        super(topicId, new CustomerKeyedSerializationSchema((SerializationMetricWrapper)serializationSchema, parititonKeys), producerConfig, customPartitioner);
        this.serializationMetricWrapper = (SerializationMetricWrapper) serializationSchema;
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        RuntimeContext runtimeContext = getRuntimeContext();
        serializationMetricWrapper.setRuntimeContext(runtimeContext);
        serializationMetricWrapper.initMetric();
        super.open(configuration);
    }

}