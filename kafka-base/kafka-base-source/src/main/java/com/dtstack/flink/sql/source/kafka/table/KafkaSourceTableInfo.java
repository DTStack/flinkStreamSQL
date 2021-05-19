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


package com.dtstack.flink.sql.source.kafka.table;

import com.dtstack.flink.sql.table.AbstractSourceTableInfo;
import com.google.common.base.Preconditions;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Reason:
 * Date: 2018/09/18
 * Company: www.dtstack.com
 * @author sishu.yss
 */

public class KafkaSourceTableInfo extends AbstractSourceTableInfo {

    public static final String BOOTSTRAPSERVERS_KEY = "bootstrapServers";

    public static final String TOPIC_KEY = "topic";

    public static final String TYPE_KEY = "type";

    public static final String GROUPID_KEY = "groupId";

    public static final String OFFSETRESET_KEY = "offsetReset";

    public static final String OFFSET_END_KEY = "offsetEnd";

    public static final String TOPICISPATTERN_KEY = "topicIsPattern";

    public static final String SCHEMA_STRING_KEY = "schemaInfo";

    public static final String CSV_FIELD_DELIMITER_KEY = "fieldDelimiter";

    public static final String SOURCE_DATA_TYPE_KEY = "sourceDataType";

    public static final String CHARSET_NAME_KEY = "charsetName";

    public static final String TIMESTAMP_OFFSET = "timestampOffset";

    public static final String KEY_DESERIALIZER = "key.deserializer";

    public static final String DT_KEY_DESERIALIZER = "dt.key.deserializer";

    public static final String VALUE_DESERIALIZER = "value.deserializer";

    public static final String DT_VALUE_DESERIALIZER = "dt.value.deserializer";

    public static final String DT_DESERIALIZER_CLASS_NAME = "com.dtstack.flink.sql.source.kafka.deserializer.DtKafkaDeserializer";

    private String bootstrapServers;

    private String topic;

    private String groupId;

    private String offsetReset;

    private Boolean topicIsPattern = false;

    private String sourceDataType;

    private String schemaString;

    private String fieldDelimiter;

    public Map<String, String> kafkaParams = new HashMap<>();

    public String charsetName;

    private Long timestampOffset;

    private Map<KafkaTopicPartition, Long> specificEndOffsets;

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

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getOffsetReset() {
        return offsetReset;
    }

    public void setOffsetReset(String offsetReset) {
        this.offsetReset = offsetReset;
    }

    public Boolean getTopicIsPattern() {
        return topicIsPattern;
    }

    public void setTopicIsPattern(Boolean topicIsPattern) {
        this.topicIsPattern = topicIsPattern;
    }

    public void addKafkaParam(Map<String, String> kafkaParam) {
        kafkaParams.putAll(kafkaParam);
    }

    public String getKafkaParam(String key) {
        return kafkaParams.get(key);
    }

    public void putKafkaParam(String key, String value) {
        kafkaParams.put(key, value);
    }

    public Set<String> getKafkaParamKeys() {
        return kafkaParams.keySet();
    }

    public String getSourceDataType() {
        return sourceDataType;
    }

    public void setSourceDataType(String sourceDataType) {
        this.sourceDataType = sourceDataType;
    }

    public String getSchemaString() {
        return schemaString;
    }

    public void setSchemaString(String schemaString) {
        this.schemaString = schemaString;
    }

    public String getFieldDelimiter() {
        return fieldDelimiter;
    }

    public void setFieldDelimiter(String fieldDelimiter) {
        this.fieldDelimiter = fieldDelimiter;
    }

	public String getCharsetName() {
		return charsetName;
	}

	public void setCharsetName(String charsetName) {
		this.charsetName = charsetName;
	}

    public Long getTimestampOffset() {
        return timestampOffset;
    }

    public void setTimestampOffset(Long timestampOffset) {
        this.timestampOffset = timestampOffset;
    }

    public Map<KafkaTopicPartition, Long> getSpecificEndOffsets() {
        return specificEndOffsets;
    }

    public void setSpecificEndOffsets(Map<KafkaTopicPartition, Long> specificEndOffsets) {
        this.specificEndOffsets = specificEndOffsets;
    }

    @Override
	public boolean check() {
		Preconditions.checkNotNull(getType(), "kafka of type is required");
		Preconditions.checkNotNull(bootstrapServers, "kafka of bootstrapServers is required");
		Preconditions.checkNotNull(topic, "kafka of topic is required");
		return false;
	}
}
