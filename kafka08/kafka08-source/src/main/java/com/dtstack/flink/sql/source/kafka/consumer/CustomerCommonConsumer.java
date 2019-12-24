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
package com.dtstack.flink.sql.source.kafka.consumer;

import com.dtstack.flink.sql.format.AbsDeserialization;
import com.dtstack.flink.sql.source.kafka.deserialization.CustomerCommonDeserialization;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.types.Row;

import java.util.Properties;
import java.util.regex.Pattern;

/**
 *
 * Date: 2018/12/18
 * Company: www.dtstack.com
 * @author DocLi
 *
 * @modifyer maqi
 */
public class CustomerCommonConsumer extends FlinkKafkaConsumer08<Row> {

	private CustomerCommonDeserialization customerCommonDeserialization;


	public CustomerCommonConsumer(String topic, AbsDeserialization<Row> deserializer, Properties props) {
		super(topic, deserializer, props);
		this.customerCommonDeserialization= (CustomerCommonDeserialization) deserializer;
	}

	public CustomerCommonConsumer(Pattern subscriptionPattern, AbsDeserialization<Row> deserializer, Properties props) {
		super(subscriptionPattern, deserializer, props);
		this.customerCommonDeserialization= (CustomerCommonDeserialization) deserializer;
	}


	@Override
	public void run(SourceFunction.SourceContext<Row> sourceContext) throws Exception {
		customerCommonDeserialization.setRuntimeContext(getRuntimeContext());
		customerCommonDeserialization.initMetric();
		super.run(sourceContext);
	}

}
