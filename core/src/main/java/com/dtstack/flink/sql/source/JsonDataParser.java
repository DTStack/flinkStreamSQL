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

package com.dtstack.flink.sql.source;

import com.dtstack.flink.sql.table.TableInfo;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.*;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 *   source data parse to json format
 *
 * Date: 2019/12/12
 * Company: www.dtstack.com
 * @author maqi
 */
public class JsonDataParser implements Serializable {

    private final ObjectMapper objectMapper = new ObjectMapper();

    private Map<String, String> rowAndFieldMapping;
    private Map<String, JsonNode> nodeAndJsonNodeMapping = Maps.newHashMap();

    private final String[] fieldNames;
    private final TypeInformation<?>[] fieldTypes;
    private List<TableInfo.FieldExtraInfo> fieldExtraInfos;

    public JsonDataParser(TypeInformation<Row> typeInfo, Map<String, String> rowAndFieldMapping, List<TableInfo.FieldExtraInfo> fieldExtraInfos) {
        this.fieldNames = ((RowTypeInfo) typeInfo).getFieldNames();
        this.fieldTypes = ((RowTypeInfo) typeInfo).getFieldTypes();
        this.rowAndFieldMapping = rowAndFieldMapping;
        this.fieldExtraInfos = fieldExtraInfos;
    }


    public Row parseData(byte[] data) throws IOException {
        JsonNode root = objectMapper.readTree(data);
        parseTree(root, null);
        Row row = new Row(fieldNames.length);

        try {
            for (int i = 0; i < fieldNames.length; i++) {
                JsonNode node = getIgnoreCase(fieldNames[i]);
                TableInfo.FieldExtraInfo fieldExtraInfo = fieldExtraInfos.get(i);

                if (node == null) {
                    if (fieldExtraInfo != null && fieldExtraInfo.getNotNull()) {
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
            return row;
        } finally {
            nodeAndJsonNodeMapping.clear();
        }
    }

    private void parseTree(JsonNode jsonNode, String prefix){
        if (jsonNode.isArray()) {
            ArrayNode array = (ArrayNode) jsonNode;
            for (int i = 0; i < array.size(); i++) {
                JsonNode child = array.get(i);
                String nodeKey = getNodeKey(prefix, i);

                if (child.isValueNode()) {
                    nodeAndJsonNodeMapping.put(nodeKey, child);
                } else {
                    if (rowAndFieldMapping.containsValue(nodeKey)) {
                        nodeAndJsonNodeMapping.put(nodeKey, child);
                    }
                    parseTree(child, nodeKey);
                }
            }
            return;
        }
        Iterator<String> iterator = jsonNode.fieldNames();
        while (iterator.hasNext()){
            String next = iterator.next();
            JsonNode child = jsonNode.get(next);
            String nodeKey = getNodeKey(prefix, next);

            if (child.isValueNode()){
                nodeAndJsonNodeMapping.put(nodeKey, child);
            }else if(child.isArray()){
                parseTree(child, nodeKey);
            }else {
                parseTree(child, nodeKey);
            }
        }
    }

    private JsonNode getIgnoreCase(String key) {
        String nodeMappingKey = rowAndFieldMapping.getOrDefault(key, key);
        return nodeAndJsonNodeMapping.get(nodeMappingKey);
    }

    private String getNodeKey(String prefix, String nodeName){
        if(Strings.isNullOrEmpty(prefix)){
            return nodeName;
        }
        return prefix + "." + nodeName;
    }

    private String getNodeKey(String prefix, int i) {
        if (Strings.isNullOrEmpty(prefix)) {
            return "[" + i + "]";
        }
        return prefix + "[" + i + "]";
    }

    private Object convert(JsonNode node, TypeInformation<?> info) {
        if (info.getTypeClass().equals(Types.BOOLEAN.getTypeClass())) {
            return node.asBoolean();
        } else if (info.getTypeClass().equals(Types.STRING.getTypeClass())) {
            if (node instanceof ObjectNode) {
                return node.toString();
            } else if (node instanceof NullNode) {
                return null;
            } else {
                return node.asText();
            }
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
