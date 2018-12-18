/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flink.sql.source.kafka.deserialization;

import com.dtstack.flink.sql.source.AbsDeserialization;
import com.dtstack.flink.sql.source.kafka.metric.KafkaTopicPartitionLagMetric;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.connectors.kafka.internal.KafkaConsumerThread;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Set;

import static com.dtstack.flink.sql.metric.MetricConstant.*;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 *
 * Date: 2018/12/18
 * Company: www.dtstack.com
 *
 * @author DocLi
 *
 * @modifyer maqi
 *
 */
public class CustomerCommonDeserialization extends AbsDeserialization<Row> implements KeyedDeserializationSchema<Row> {
	private static final Logger LOG = LoggerFactory.getLogger(CustomerCommonDeserialization.class);

	public static final String[] KAFKA_COLUMNS = new String[]{"_TOPIC", "_MESSAGEKEY", "_MESSAGE", "_PARTITION", "_OFFSET"};

	private AbstractFetcher<Row, ?> fetcher;

	private boolean firstMsg = true;

	@Override
	public Row deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) {
		if (firstMsg) {
			try {
				registerPtMetric(fetcher);
			} catch (Exception e) {
				LOG.error("register topic partition metric error.", e);
			}
			firstMsg = false;
		}

		numInRecord.inc();
		numInBytes.inc(message.length);
		numInBytes.inc(messageKey.length);

		try {
			Row row = Row.of(
					topic, //topic
					messageKey == null ? null : new String(messageKey, UTF_8), //key
					new String(message, UTF_8), //message
					partition,
					offset
			);
			return row;
		} catch (Throwable t) {
			LOG.error(t.getMessage());
			dirtyDataCounter.inc();
			return null;
		}
	}

	@Override
	public Row deserialize(byte[] message) throws IOException {
		return null;
	}

	public void setFetcher(AbstractFetcher<Row, ?> fetcher) {
		this.fetcher = fetcher;
	}


	@Override
	public boolean isEndOfStream(Row nextElement) {
		return false;
	}

	public TypeInformation<Row> getProducedType() {
		TypeInformation<?>[] types = new TypeInformation<?>[]{
				TypeExtractor.createTypeInfo(String.class),
				TypeExtractor.createTypeInfo(String.class), //createTypeInformation[String]
				TypeExtractor.createTypeInfo(String.class),
				Types.INT,
				Types.LONG
		};
		return new RowTypeInfo(types, KAFKA_COLUMNS);
	}

	protected void registerPtMetric(AbstractFetcher<Row, ?> fetcher) throws Exception {

		Field consumerThreadField = fetcher.getClass().getSuperclass().getDeclaredField("consumerThread");
		consumerThreadField.setAccessible(true);
		KafkaConsumerThread consumerThread = (KafkaConsumerThread) consumerThreadField.get(fetcher);

		Field hasAssignedPartitionsField = consumerThread.getClass().getDeclaredField("hasAssignedPartitions");
		hasAssignedPartitionsField.setAccessible(true);

		//wait until assignedPartitions

		boolean hasAssignedPartitions = (boolean) hasAssignedPartitionsField.get(consumerThread);

		if (!hasAssignedPartitions) {
			throw new RuntimeException("wait 50 secs, but not assignedPartitions");
		}

		Field consumerField = consumerThread.getClass().getDeclaredField("consumer");
		consumerField.setAccessible(true);

		KafkaConsumer kafkaConsumer = (KafkaConsumer) consumerField.get(consumerThread);
		Field subscriptionStateField = kafkaConsumer.getClass().getDeclaredField("subscriptions");
		subscriptionStateField.setAccessible(true);

		//topic partitions lag
		SubscriptionState subscriptionState = (SubscriptionState) subscriptionStateField.get(kafkaConsumer);
		Set<TopicPartition> assignedPartitions = subscriptionState.assignedPartitions();
		for (TopicPartition topicPartition : assignedPartitions) {
			MetricGroup metricGroup = getRuntimeContext().getMetricGroup().addGroup(DT_TOPIC_GROUP, topicPartition.topic())
					.addGroup(DT_PARTITION_GROUP, topicPartition.partition() + "");
			metricGroup.gauge(DT_TOPIC_PARTITION_LAG_GAUGE, new KafkaTopicPartitionLagMetric(subscriptionState, topicPartition));
		}

	}
}
