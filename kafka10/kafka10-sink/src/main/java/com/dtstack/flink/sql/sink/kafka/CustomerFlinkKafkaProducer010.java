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

import com.dtstack.flink.sql.metric.MetricConstant;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.internals.metrics.KafkaMetricWrapper;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaDelegatePartitioner;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import java.util.Map;
import java.util.Properties;

/**
 * Reason:
 * Date: 2019/4/24
 * Company: www.dtstack.com
 *
 * @author maqi
 */
public class CustomerFlinkKafkaProducer010<Row> extends FlinkKafkaProducer010<Row> {

	CustomerJsonRowSerializationSchema schema;

	public CustomerFlinkKafkaProducer010(String topicId, SerializationSchema<Row> serializationSchema, Properties producerConfig) {
		super(topicId, serializationSchema, producerConfig);
		this.schema = (CustomerJsonRowSerializationSchema) serializationSchema;
	}

	@Override
	public void open(Configuration configuration) {
		producer = getKafkaProducer(this.producerConfig);

		RuntimeContext ctx = getRuntimeContext();
		Counter counter = ctx.getMetricGroup().counter(MetricConstant.DT_NUM_RECORDS_OUT);
		MeterView meter = ctx.getMetricGroup().meter(MetricConstant.DT_NUM_RECORDS_OUT_RATE, new MeterView(counter, 20));

		schema.setCounter(counter);

		if (null != flinkKafkaPartitioner) {
			if (flinkKafkaPartitioner instanceof FlinkKafkaDelegatePartitioner) {
				((FlinkKafkaDelegatePartitioner) flinkKafkaPartitioner).setPartitions(
						getPartitionsByTopic(this.defaultTopicId, this.producer));
			}
			flinkKafkaPartitioner.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks());
		}


		// register Kafka metrics to Flink accumulators
		if (!Boolean.parseBoolean(producerConfig.getProperty(KEY_DISABLE_METRICS, "false"))) {
			Map<MetricName, ? extends Metric> metrics = this.producer.metrics();

			if (metrics == null) {
				// MapR's Kafka implementation returns null here.
			} else {
				final MetricGroup kafkaMetricGroup = getRuntimeContext().getMetricGroup().addGroup("KafkaProducer");
				for (Map.Entry<MetricName, ? extends Metric> metric : metrics.entrySet()) {
					kafkaMetricGroup.gauge(metric.getKey().name(), new KafkaMetricWrapper(metric.getValue()));
				}
			}
		}

		if (flushOnCheckpoint && !((StreamingRuntimeContext) this.getRuntimeContext()).isCheckpointingEnabled()) {
			flushOnCheckpoint = false;
		}

		if (logFailuresOnly) {
			callback = new Callback() {
				@Override
				public void onCompletion(RecordMetadata metadata, Exception e) {
					if (e != null) {
					}
					acknowledgeMessage();
				}
			};
		} else {
			callback = new Callback() {
				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if (exception != null && asyncException == null) {
						asyncException = exception;
					}
					acknowledgeMessage();
				}
			};
		}
	}

	private void acknowledgeMessage() {
		if (flushOnCheckpoint) {
			synchronized (pendingRecordsLock) {
				pendingRecords--;
				if (pendingRecords == 0) {
					pendingRecordsLock.notifyAll();
				}
			}
		}
	}
}
