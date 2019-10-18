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


import com.dtstack.flink.sql.source.AbsDeserialization;
import com.dtstack.flink.sql.source.kafka.metric.KafkaTopicPartitionLagMetric;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.calcite.shaded.com.google.common.base.Strings;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.JsonNodeType;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.TextNode;
import org.apache.flink.streaming.connectors.kafka.internal.KafkaConsumerThread;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.SubscriptionState;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static com.dtstack.flink.sql.metric.MetricConstant.*;

/**
 * json string parsing custom
 * Date: 2018/09/18
 * Company: www.dtstack.com
 * @author sishu.yss
 */

public class CustomerJsonDeserialization extends AbsDeserialization<Row> {

    private static final Logger LOG = LoggerFactory.getLogger(CustomerJsonDeserialization.class);

    private static final long serialVersionUID = 2385115520960444192L;

    private static int dirtyDataFrequency = 1000;

    private final ObjectMapper objectMapper = new ObjectMapper();

    /** Type information describing the result type. */
    private final TypeInformation<Row> typeInfo;

    /** Field names to parse. Indices match fieldTypes indices. */
    private final String[] fieldNames;

    /** Types to parse fields as. Indices match fieldNames indices. */
    private final TypeInformation<?>[] fieldTypes;

    /** Flag indicating whether to fail on a missing field. */
    private boolean failOnMissingField;

    private AbstractFetcher<Row, ?> fetcher;

    private boolean firstMsg = true;

    private Map<String, JsonNode> nodeAndJsonNodeMapping = Maps.newHashMap();

    private Map<String, String> rowAndFieldMapping;

    public CustomerJsonDeserialization(TypeInformation<Row> typeInfo, Map<String, String> rowAndFieldMapping){
        this.typeInfo = typeInfo;

        this.fieldNames = ((RowTypeInfo) typeInfo).getFieldNames();

        this.fieldTypes = ((RowTypeInfo) typeInfo).getFieldTypes();

        this.rowAndFieldMapping= rowAndFieldMapping;
    }

    @Override
    public Row deserialize(byte[] message) throws IOException {

        if(firstMsg){
            try {
                registerPtMetric(fetcher);
            } catch (Exception e) {
                LOG.error("register topic partition metric error.", e);
            }

            firstMsg = false;
        }

        try {
            JsonNode root = objectMapper.readTree(message);

            if (numInRecord.getCount() % dirtyDataFrequency == 0) {
                LOG.info(root.toString());
            }

            numInRecord.inc();
            numInBytes.inc(message.length);

            parseTree(root, null);
            Row row = new Row(fieldNames.length);

            for (int i = 0; i < fieldNames.length; i++) {
                JsonNode node = getIgnoreCase(fieldNames[i]);

                if (node == null) {
                    if (failOnMissingField) {
                        throw new IllegalStateException("Failed to find field with name '"
                                + fieldNames[i] + "'.");
                    } else {
                        row.setField(i, null);
                    }
                } else {
                    // Read the value as specified type
                    Object value = convert(node, fieldTypes[i]);
                    row.setField(i, value);
                }
            }

            numInResolveRecord.inc();
            return row;
        } catch (Exception e) {
            //add metric of dirty data
            if (dirtyDataCounter.getCount() % dirtyDataFrequency == 0 || LOG.isDebugEnabled()) {
                LOG.info("dirtyData: " + new String(message));
                LOG.error(" ", e);
            }
            dirtyDataCounter.inc();
            return null;
        }finally {
            nodeAndJsonNodeMapping.clear();
        }
    }

    public JsonNode getIgnoreCase(String key) {
        String nodeMappingKey = rowAndFieldMapping.getOrDefault(key, key);
        JsonNode node = nodeAndJsonNodeMapping.get(nodeMappingKey);

        if(node == null){
            return null;
        }

        JsonNodeType nodeType = node.getNodeType();

        if (nodeType==JsonNodeType.ARRAY){
            throw new IllegalStateException("Unsupported  type information  array .") ;
        }

        return node;
    }


    public void setFailOnMissingField(boolean failOnMissingField) {
        this.failOnMissingField = failOnMissingField;
    }

    private void parseTree(JsonNode jsonNode, String prefix){

        Iterator<String> iterator = jsonNode.fieldNames();
        while (iterator.hasNext()){
            String next = iterator.next();
            JsonNode child = jsonNode.get(next);
            String nodeKey = getNodeKey(prefix, next);

            if (child.isValueNode()){
                nodeAndJsonNodeMapping.put(nodeKey, child);
            }else if(child.isArray()){
                nodeAndJsonNodeMapping.put(nodeKey, new TextNode(child.toString()));
            }else {
                parseTree(child, nodeKey);
            }
        }
    }

    private String getNodeKey(String prefix, String nodeName){
        if(Strings.isNullOrEmpty(prefix)){
            return nodeName;
        }

        return prefix + "." + nodeName;
    }

    public void setFetcher(AbstractFetcher<Row, ?> fetcher) {
        this.fetcher = fetcher;
    }


    protected void registerPtMetric(AbstractFetcher<Row, ?> fetcher) throws Exception {

        Field consumerThreadField = fetcher.getClass().getSuperclass().getDeclaredField("consumerThread");
        consumerThreadField.setAccessible(true);
        KafkaConsumerThread consumerThread = (KafkaConsumerThread) consumerThreadField.get(fetcher);

        Field hasAssignedPartitionsField = consumerThread.getClass().getDeclaredField("hasAssignedPartitions");
        hasAssignedPartitionsField.setAccessible(true);

        //wait until assignedPartitions

        boolean hasAssignedPartitions = (boolean) hasAssignedPartitionsField.get(consumerThread);

        if(!hasAssignedPartitions){
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
        for(TopicPartition topicPartition : assignedPartitions){
            MetricGroup metricGroup = getRuntimeContext().getMetricGroup().addGroup(DT_TOPIC_GROUP, topicPartition.topic())
                    .addGroup(DT_PARTITION_GROUP, topicPartition.partition() + "");
            metricGroup.gauge(DT_TOPIC_PARTITION_LAG_GAUGE, new KafkaTopicPartitionLagMetric(subscriptionState, topicPartition));
        }

    }

    private static String partitionLagMetricName(TopicPartition tp) {
        return tp + ".records-lag";
    }

    private Object convert(JsonNode node, TypeInformation<?> info) {
        if (info.getTypeClass().equals(Types.BOOLEAN.getTypeClass())) {
            return node.asBoolean();
        } else if (info.getTypeClass().equals(Types.STRING.getTypeClass())) {
            return node.asText();
        }  else if (info.getTypeClass().equals(Types.SQL_DATE.getTypeClass())) {
            return Date.valueOf(node.asText());
        } else if (info.getTypeClass().equals(Types.SQL_TIME.getTypeClass())) {
            // local zone
            return Time.valueOf(node.asText());
        } else if (info.getTypeClass().equals(Types.SQL_TIMESTAMP.getTypeClass())) {
            // local zone
            return Timestamp.valueOf(node.asText());
        }  else {
            // for types that were specified without JSON schema
            // e.g. POJOs
            try {
                return objectMapper.treeToValue(node, info.getTypeClass());
            } catch (JsonProcessingException e) {
                throw new IllegalStateException("Unsupported type information '" + info + "' for node: " + node);
            }
        }
    }
}
