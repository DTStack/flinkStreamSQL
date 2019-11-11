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
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.types.Row;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Iterator;
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

    public CustomerJsonDeserialization(TypeInformation<Row> typeInfo){
        this.typeInfo = typeInfo;

        this.fieldNames = ((RowTypeInfo) typeInfo).getFieldNames();

        this.fieldTypes = ((RowTypeInfo) typeInfo).getFieldTypes();
    }

    @Override
    public Row deserialize(byte[] message) throws IOException {

        try {
           // numInRecord.inc();
           // numInBytes.inc(message.length);

            JsonNode root = objectMapper.readTree(message);
            Row row = new Row(fieldNames.length);
            for (int i = 0; i < fieldNames.length; i++) {
                JsonNode node = getIgnoreCase(root, fieldNames[i]);

                if (node == null) {
                    if (failOnMissingField) {
                        throw new IllegalStateException("Failed to find field with name '"
                                + fieldNames[i] + "'.");
                    } else {
                        row.setField(i, null);
                    }
                } else {
                    // Read the value as specified type
                    Object value = objectMapper.treeToValue(node, fieldTypes[i].getTypeClass());
                    row.setField(i, value);
                }
            }

          //  numInResolveRecord.inc();
            return row;
        } catch (Throwable t) {
            //add metric of dirty data
            LOG.error(t.getMessage());
           // dirtyDataCounter.inc();
            return null;
        }
    }

    public void setFailOnMissingField(boolean failOnMissingField) {
        this.failOnMissingField = failOnMissingField;
    }

    public JsonNode getIgnoreCase(JsonNode jsonNode, String key) {

        Iterator<String> iter = jsonNode.fieldNames();
        while (iter.hasNext()) {
            String key1 = iter.next();
            if (key1.equalsIgnoreCase(key)) {
                return jsonNode.get(key1);
            }
        }

        return null;

    }

    public void setFetcher(AbstractFetcher<Row, ?> fetcher) {
        this.fetcher = fetcher;
    }


    private static String partitionLagMetricName(TopicPartition tp) {
        return tp + ".records-lag";
    }
}
