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

package com.dtstack.flink.sql.sink.kafka;

import com.dtstack.flink.sql.sink.IStreamSinkGener;
import com.dtstack.flink.sql.sink.kafka.table.KafkaSinkTableInfo;
import com.dtstack.flink.sql.table.TargetTableInfo;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSink;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;
import java.util.Properties;

/**
 * Date: 2018/12/18
 * Company: www.dtstack.com
 *
 * @author DocLi
 * @modifyer maqi
 */
public class KafkaSink implements RetractStreamTableSink<Row>, IStreamSinkGener<KafkaSink> {

	protected String[] fieldNames;

	protected TypeInformation<?>[] fieldTypes;

	protected String topic;

	protected Properties properties;

	/** Serialization schema for encoding records to Kafka. */
	protected SerializationSchema serializationSchema;

	protected int parallelism;


	@Override
	public KafkaSink genStreamSink(TargetTableInfo targetTableInfo) {
		KafkaSinkTableInfo kafka09SinkTableInfo = (KafkaSinkTableInfo) targetTableInfo;
		this.topic = kafka09SinkTableInfo.getTopic();
		this.fieldNames = kafka09SinkTableInfo.getFields();
		TypeInformation[] types = new TypeInformation[kafka09SinkTableInfo.getFields().length];
		for (int i = 0; i < kafka09SinkTableInfo.getFieldClasses().length; i++) {
			types[i] = TypeInformation.of(kafka09SinkTableInfo.getFieldClasses()[i]);
		}
		this.fieldTypes = types;

		Integer parallelism = kafka09SinkTableInfo.getParallelism();
		if (parallelism != null) {
			this.parallelism = parallelism;
		}

		properties = new Properties();
		for (String key : kafka09SinkTableInfo.getKafkaParamKeys()) {
			properties.setProperty(key, kafka09SinkTableInfo.getKafkaParam(key));
		}
		properties.setProperty("bootstrap.servers", kafka09SinkTableInfo.getBootstrapServers());

		this.serializationSchema = new CustomerJsonRowSerializationSchema(getOutputType().getTypeAt(1));
		return this;
	}

	@Override
	public TypeInformation<Row> getRecordType() {
		return new RowTypeInfo(fieldTypes, fieldNames);
	}

	@Override
	public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
		KafkaTableSink kafkaTableSink = new CustomerKafka09JsonTableSink(
				topic,
				properties,
				serializationSchema
		);


		DataStream<Row> ds = dataStream.map((Tuple2<Boolean, Row> record) -> {
			return record.f1;
		}).returns(getOutputType().getTypeAt(1)).setParallelism(parallelism);

		kafkaTableSink.emitDataStream(ds);
	}

	@Override
	public TupleTypeInfo<Tuple2<Boolean, Row>> getOutputType() {
		return new TupleTypeInfo(org.apache.flink.table.api.Types.BOOLEAN(), new RowTypeInfo(fieldTypes, fieldNames));
	}

	@Override
	public String[] getFieldNames() {
		return fieldNames;
	}

	@Override
	public TypeInformation<?>[] getFieldTypes() {
		return fieldTypes;
	}

	@Override
	public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		this.fieldNames = fieldNames;
		this.fieldTypes = fieldTypes;
		return this;
	}

}
