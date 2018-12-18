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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Date: 2017/5/28
 *
 * @author DocLi
 */
public class CustomerCommonDeserialization extends AbsDeserialization<Row> implements KeyedDeserializationSchema<Row> {
	private static final Logger LOG = LoggerFactory.getLogger(CustomerCommonDeserialization.class);

	public static final String[] KAFKA_COLUMNS = new String[]{"_TOPIC", "_MESSAGEKEY", "_MESSAGE", "_PARTITION", "_OFFSET"};

	private boolean firstMsg = true;

	@Override
	public Row deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset) {

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

}
