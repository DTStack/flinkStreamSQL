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
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducerBase;
import org.apache.flink.streaming.connectors.kafka.Kafka010TableSink;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.utils.TableConnectorUtils;
import org.apache.flink.types.Row;

import java.util.Optional;
import java.util.Properties;

/**
 * Reason: add schema info
 * Date: 2019/4/8
 * Company: www.dtstack.com
 *
 * @author maqi
 */
public class CustomerKafka10JsonTableSink extends Kafka010TableSink {


	protected SerializationSchema schema;

	public CustomerKafka10JsonTableSink(TableSchema schema,
										String topic,
										Properties properties,
										Optional<FlinkKafkaPartitioner<Row>> partitioner,
										SerializationSchema<Row> serializationSchema) {
		super(schema, topic, properties, partitioner, serializationSchema);
		this.schema = serializationSchema;
	}



	@Override
	protected FlinkKafkaProducerBase<Row> createKafkaProducer(
			String topic,
			Properties properties,
			SerializationSchema<Row> serializationSchema,
			Optional<FlinkKafkaPartitioner<Row>> partitioner) {
		return new CustomerFlinkKafkaProducer010<Row>(topic, serializationSchema, properties);
	}


	@Override
	public void emitDataStream(DataStream<Row> dataStream) {
		SinkFunction<Row>  kafkaProducer = createKafkaProducer(topic, properties, schema, partitioner);
		// always enable flush on checkpoint to achieve at-least-once if query runs with checkpointing enabled.
		//kafkaProducer.setFlushOnCheckpoint(true);
		dataStream.addSink(kafkaProducer).name(TableConnectorUtils.generateRuntimeName(this.getClass(), getFieldNames()));

	}
}
