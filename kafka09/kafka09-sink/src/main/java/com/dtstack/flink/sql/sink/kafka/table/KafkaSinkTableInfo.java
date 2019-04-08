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

package com.dtstack.flink.sql.sink.kafka.table;

import com.dtstack.flink.sql.table.TargetTableInfo;
import org.apache.flink.calcite.shaded.com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Date: 2018/12/18
 * Company: www.dtstack.com
 *
 * @author DocLi
 * @modifyer maqi
 */
public class KafkaSinkTableInfo extends TargetTableInfo {
	//version
	private static final String CURR_TYPE = "kafka09";

	public static final String BOOTSTRAPSERVERS_KEY = "bootstrapServers";

	public static final String TOPIC_KEY = "topic";

	private String bootstrapServers;

	private String topic;

	public KafkaSinkTableInfo() {
		super.setType(CURR_TYPE);
	}


	public String getBootstrapServers() {
		return bootstrapServers;
	}

	public void setBootstrapServers(String bootstrapServers) {
		this.bootstrapServers = bootstrapServers;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	@Override
	public boolean check() {
		Preconditions.checkNotNull(bootstrapServers, "kafka of bootstrapServers is required");
		Preconditions.checkNotNull(topic, "kafka of topic is required");
		return false;
	}

	@Override
	public String getType() {
//        return super.getType() + SOURCE_SUFFIX;
		return super.getType();
	}
}
