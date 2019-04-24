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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducerBase;
import org.apache.flink.streaming.connectors.kafka.Kafka010JsonTableSink;
import org.apache.flink.streaming.connectors.kafka.KafkaJsonTableSink;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaDelegatePartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.KafkaPartitioner;
import org.apache.flink.table.util.TableConnectorUtil;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * Reason: add schema info
 * Date: 2019/4/8
 * Company: www.dtstack.com
 *
 * @author maqi
 */
public class CustomerKafka10JsonTableSink extends KafkaJsonTableSink {


	protected SerializationSchema schema;

	public CustomerKafka10JsonTableSink(String topic, Properties properties, SerializationSchema schema) {
		super(topic, properties, new FlinkFixedPartitioner<>());
		this.schema = schema;
	}

	public CustomerKafka10JsonTableSink(String topic, Properties properties, FlinkKafkaPartitioner<Row> partitioner, SerializationSchema schema) {
		super(topic, properties, partitioner);
		this.schema = schema;
	}


	@Deprecated
	public CustomerKafka10JsonTableSink(String topic, Properties properties, KafkaPartitioner<Row> partitioner, SerializationSchema schema) {
		super(topic, properties, new FlinkKafkaDelegatePartitioner<>(partitioner));
		this.schema = schema;
	}

	@Override
	protected FlinkKafkaProducerBase<Row> createKafkaProducer(String topic, Properties properties, SerializationSchema<Row> serializationSchema, FlinkKafkaPartitioner<Row> partitioner) {
		return new FlinkKafkaProducer010<Row>(topic, serializationSchema, properties, partitioner);
	}

	@Override
	protected Kafka010JsonTableSink createCopy() {
		return new Kafka010JsonTableSink(topic, properties, partitioner);
	}

	@Override
	public void emitDataStream(DataStream<Row> dataStream) {
		FlinkKafkaProducerBase<Row> kafkaProducer = createKafkaProducer(topic, properties, schema, partitioner);
		// always enable flush on checkpoint to achieve at-least-once if query runs with checkpointing enabled.
		kafkaProducer.setFlushOnCheckpoint(true);
		dataStream.addSink(kafkaProducer).name(TableConnectorUtil.generateRuntimeName(this.getClass(), fieldNames));
	}
}
