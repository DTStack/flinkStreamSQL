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

package com.dtstack.flink.sql.format;

import com.dtstack.flink.sql.metric.MetricConstant;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.types.Row;


/**
 * add metric for source
 * <p>
 * company: www.dtstack.com
 * author: toutian
 * create: 2019/12/24
 */
public class SerializationMetricWrapper implements SerializationSchema<Row> {

    private SerializationSchema<Row> serializationSchema;

    private transient RuntimeContext runtimeContext;

    protected transient Counter dtNumRecordsOut;

    protected transient Meter dtNumRecordsOutRate;


    public SerializationMetricWrapper(SerializationSchema<Row> serializationSchema) {
        this.serializationSchema = serializationSchema;
    }

    public void initMetric() {
        dtNumRecordsOut = runtimeContext.getMetricGroup().counter(MetricConstant.DT_NUM_RECORDS_OUT);
        dtNumRecordsOutRate = runtimeContext.getMetricGroup().meter(MetricConstant.DT_NUM_RECORDS_OUT_RATE, new MeterView(dtNumRecordsOut, 20));
    }

    @Override
    public byte[] serialize(Row element) {
        beforeSerialize();
        byte[] row = serializationSchema.serialize(element);
        afterSerialize();
        return row;
    }

    protected void beforeSerialize() {
    }

    protected void afterSerialize() {
        dtNumRecordsOut.inc();
    }

    public RuntimeContext getRuntimeContext() {
        return runtimeContext;
    }

    public void setRuntimeContext(RuntimeContext runtimeContext) {
        this.runtimeContext = runtimeContext;
    }

    public SerializationSchema<Row> getSerializationSchema() {
        return serializationSchema;
    }

}
