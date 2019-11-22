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

import com.dtstack.flink.sql.table.SourceTableInfo;
import org.apache.flink.calcite.shaded.com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author: chuixue
 * @create: 2019-11-05 11:09
 * @description:
 **/
public class KafkaSourceTableInfo extends SourceTableInfo {

    //version
    private static final String CURR_TYPE = "kafka";

    public static final String BOOTSTRAPSERVERS_KEY = "bootstrapServers";

    public static final String TOPIC_KEY = "topic";

    public static final String GROUPID_KEY = "groupId";

    public static final String OFFSETRESET_KEY = "offsetReset";

    public static final String TOPICISPATTERN_KEY = "topicIsPattern";

    private String bootstrapServers;

    private String topic;

    private String groupId;

    private Boolean topicIsPattern = false;

    public Boolean getTopicIsPattern() {
        return topicIsPattern;
    }

    public void setTopicIsPattern(Boolean topicIsPattern) {
        if (topicIsPattern == null) return;

        this.topicIsPattern = topicIsPattern;
    }

    //latest, earliest
    private String offsetReset = "latest";

    private String offset;

    public KafkaSourceTableInfo() {
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
        if (offsetReset == null) {
            return;
        }

        this.offsetReset = offsetReset;
    }

    public String getOffset() {
        return offset;
    }

    public void setOffset(String offset) {
        this.offset = offset;
    }

    public Map<String, String> kafkaParam = new HashMap<>();

    public void addKafkaParam(String key, String value) {
        kafkaParam.put(key, value);
    }

    public String getKafkaParam(String key) {
        return kafkaParam.get(key);
    }

    public Set<String> getKafkaParamKeys() {
        return kafkaParam.keySet();
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
