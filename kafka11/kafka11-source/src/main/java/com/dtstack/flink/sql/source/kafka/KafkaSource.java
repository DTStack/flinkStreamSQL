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


package com.dtstack.flink.sql.source.kafka;

import com.dtstack.flink.sql.source.IStreamSourceGener;
import com.dtstack.flink.sql.source.kafka.table.KafkaSourceTableInfo;
import com.dtstack.flink.sql.table.SourceTableInfo;
import com.dtstack.flink.sql.util.DtStringUtil;
import com.dtstack.flink.sql.util.PluginUtil;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * If eventtime field is specified, the default time field rowtime
 * Date: 2018/09/18
 * Company: www.dtstack.com
 *
 * @author sishu.yss
 */

public class KafkaSource implements IStreamSourceGener<Table> {

	private static final String SOURCE_OPERATOR_NAME_TPL = "${topic}_${table}";

	/**
	 * Get kafka data source, you need to provide the data field names, data types
	 * If you do not specify auto.offset.reset, the default use groupoffset
	 *
	 * @param sourceTableInfo
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public Table genStreamSource(SourceTableInfo sourceTableInfo, StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {

		KafkaSourceTableInfo kafka011SourceTableInfo = (KafkaSourceTableInfo) sourceTableInfo;
		String topicName = kafka011SourceTableInfo.getTopic();

		Properties props = new Properties();
		for (String key : kafka011SourceTableInfo.getKafkaParamKeys()) {
			props.setProperty(key, kafka011SourceTableInfo.getKafkaParam(key));
		}
		props.setProperty("bootstrap.servers", kafka011SourceTableInfo.getBootstrapServers());
		if (DtStringUtil.isJosn(kafka011SourceTableInfo.getOffsetReset())){
			props.setProperty("auto.offset.reset", "none");
		} else {
			props.setProperty("auto.offset.reset", kafka011SourceTableInfo.getOffsetReset());
		}
		if (StringUtils.isNotBlank(kafka011SourceTableInfo.getGroupId())){
			props.setProperty("group.id", kafka011SourceTableInfo.getGroupId());
		}
		// only required for Kafka 0.8
		//TODO props.setProperty("zookeeper.connect", kafka09SourceTableInfo.)

		TypeInformation[] types = new TypeInformation[kafka011SourceTableInfo.getFields().length];
		for (int i = 0; i < kafka011SourceTableInfo.getFieldClasses().length; i++) {
			types[i] = TypeInformation.of(kafka011SourceTableInfo.getFieldClasses()[i]);
		}

		TypeInformation<Row> typeInformation = new RowTypeInfo(types, kafka011SourceTableInfo.getFields());

		FlinkKafkaConsumer011<Row> kafkaSrc;
		if (BooleanUtils.isTrue(kafka011SourceTableInfo.getTopicIsPattern())) {
			kafkaSrc = new CustomerKafka011Consumer(Pattern.compile(topicName),
					new CustomerJsonDeserialization(typeInformation, kafka011SourceTableInfo.getPhysicalFields(), kafka011SourceTableInfo.getFieldExtraInfoList()), props);
		} else {
			kafkaSrc = new CustomerKafka011Consumer(topicName,
					new CustomerJsonDeserialization(typeInformation, kafka011SourceTableInfo.getPhysicalFields(), kafka011SourceTableInfo.getFieldExtraInfoList()), props);
		}

		//earliest,latest
		if ("earliest".equalsIgnoreCase(kafka011SourceTableInfo.getOffsetReset())) {
			kafkaSrc.setStartFromEarliest();
		} else if (DtStringUtil.isJosn(kafka011SourceTableInfo.getOffsetReset())) {// {"0":12312,"1":12321,"2":12312}
			try {
				Properties properties = PluginUtil.jsonStrToObject(kafka011SourceTableInfo.getOffsetReset(), Properties.class);
				Map<String, Object> offsetMap = PluginUtil.ObjectToMap(properties);
				Map<KafkaTopicPartition, Long> specificStartupOffsets = new HashMap<>();
				for (Map.Entry<String, Object> entry : offsetMap.entrySet()) {
					specificStartupOffsets.put(new KafkaTopicPartition(topicName, Integer.valueOf(entry.getKey())), Long.valueOf(entry.getValue().toString()));
				}
				kafkaSrc.setStartFromSpecificOffsets(specificStartupOffsets);
			} catch (Exception e) {
				throw new RuntimeException("not support offsetReset type:" + kafka011SourceTableInfo.getOffsetReset());
			}
		} else {
			kafkaSrc.setStartFromLatest();
		}

		String fields = StringUtils.join(kafka011SourceTableInfo.getFields(), ",");
		String sourceOperatorName = SOURCE_OPERATOR_NAME_TPL.replace("${topic}", topicName).replace("${table}", sourceTableInfo.getName());

		DataStreamSource kafkaSource = env.addSource(kafkaSrc, sourceOperatorName, typeInformation);
		Integer parallelism = kafka011SourceTableInfo.getParallelism();
		if (parallelism > 0) {
			kafkaSource.setParallelism(parallelism);
		}
		return tableEnv.fromDataStream(kafkaSource, fields);
	}
}
