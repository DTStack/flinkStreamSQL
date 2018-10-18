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


import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * json string parsing custom
 * Date: 2017/5/28
 * Company: www.dtstack.com
 * @author xuchao
 */

public class CustomerJsonDeserialization extends AbstractDeserializationSchema<Row> {

    private static final Logger logger = LoggerFactory.getLogger(CustomerJsonDeserialization.class);

    private final ObjectMapper objectMapper = new ObjectMapper();

    public static final String DIRTY_DATA_METRICS_COUNTER = "dirtyData";

    public static final String KAFKA_SOURCE_IN_METRIC_COUNTER = "kafkaSourceIn";

    public static final String KAFKA_SOURCE_IN_RATE_METRIC_METRE = "kafkaSourceInRate";

    public static final String KAFKA_SOURCE_IN_BYTES_METRIC_COUNTER = "kafkaSourceInBytes";

    public static final String KAFKA_SOURCE_IN_BYTES_RATE_METRIC_METRE = "kafkaSourceInBytesRate";

    public static final String KAFKA_SOURCE_IN_RESOLVE_METRIC_COUNTER = "kafkaSourceInResolve";

    public static final String KAFKA_SOURCE_IN_RESOLVE_RATE_METRIC_METRE = "kafkaSourceInResolveRate";

    /** Type information describing the result type. */
    private final TypeInformation<Row> typeInfo;

    private transient RuntimeContext runtimeContext;

    /** Field names to parse. Indices match fieldTypes indices. */
    private final String[] fieldNames;

    /** Types to parse fields as. Indices match fieldNames indices. */
    private final TypeInformation<?>[] fieldTypes;

    /** Flag indicating whether to fail on a missing field. */
    private boolean failOnMissingField;

    private transient Counter dirtyDataCounter;

    //tps ransactions Per Second
    private transient Counter kafkaInRecord;

    private transient Meter kafkaInRate;

    //rps Record Per Second: deserialize data and out record num
    private transient Counter kafkaInResolveRecord;

    private transient Meter kafkaInResolveRate;

    private transient Counter kafkaInBytes;

    private transient Meter kafkaInBytesRate;

    //FIXME just for test
    private LongCounter myCounter;

    public CustomerJsonDeserialization(TypeInformation<Row> typeInfo){
        this.typeInfo = typeInfo;

        this.fieldNames = ((RowTypeInfo) typeInfo).getFieldNames();

        this.fieldTypes = ((RowTypeInfo) typeInfo).getFieldTypes();
    }

    @Override
    public Row deserialize(byte[] message) throws IOException {
        try {
            myCounter.add(1);
            kafkaInRecord.inc();
            kafkaInBytes.inc(message.length);
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

            kafkaInResolveRecord.inc();
            return row;
        } catch (Throwable t) {
            //add metric of dirty data
            dirtyDataCounter.inc();
            return new Row(fieldNames.length);
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

    public RuntimeContext getRuntimeContext() {
        return runtimeContext;
    }

    public void setRuntimeContext(RuntimeContext runtimeContext) {
        this.runtimeContext = runtimeContext;
    }

    public void initMetric(){
        dirtyDataCounter = runtimeContext.getMetricGroup().counter(DIRTY_DATA_METRICS_COUNTER);

        kafkaInRecord = runtimeContext.getMetricGroup().counter(KAFKA_SOURCE_IN_METRIC_COUNTER);
        kafkaInRate = runtimeContext.getMetricGroup().meter( KAFKA_SOURCE_IN_RATE_METRIC_METRE, new MeterView(kafkaInRecord, 20));

        kafkaInBytes = runtimeContext.getMetricGroup().counter(KAFKA_SOURCE_IN_BYTES_METRIC_COUNTER);
        kafkaInBytesRate = runtimeContext.getMetricGroup().meter( KAFKA_SOURCE_IN_BYTES_RATE_METRIC_METRE, new MeterView(kafkaInBytes, 20));

        kafkaInResolveRecord = runtimeContext.getMetricGroup().counter(KAFKA_SOURCE_IN_RESOLVE_METRIC_COUNTER);
        kafkaInResolveRate = runtimeContext.getMetricGroup().meter(KAFKA_SOURCE_IN_RESOLVE_RATE_METRIC_METRE, new MeterView(kafkaInResolveRecord, 20));

        //FIXME
        myCounter = runtimeContext.getLongCounter("kafkaSourceTotalIn");
    }
}
