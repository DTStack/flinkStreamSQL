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

package com.dtstack.flink.sql.sink.elasticsearch;

import com.dtstack.flink.sql.metric.MetricConstant;
import com.dtstack.flink.sql.sink.elasticsearch.table.ElasticsearchTableInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.util.NoOpFailureHandler;

import java.util.List;
import java.util.Map;

/**
 * @Auther: jiangjunjie
 * @Date: 2018/11/29 14:15
 * @Description:
 *
 */
public class MetricElasticsearchSink extends ElasticsearchSinkBase {

    protected CustomerSinkFunc customerSinkFunc;

    protected transient Meter outRecordsRate;

    protected Map userConfig;


    public MetricElasticsearchSink(Map userConfig, List transportAddresses,
                                   ElasticsearchSinkFunction elasticsearchSinkFunction,
                                   ElasticsearchTableInfo esTableInfo) {
        super(new ExtendES5ApiCallBridge(transportAddresses, esTableInfo), userConfig, elasticsearchSinkFunction, new NoOpFailureHandler());
        this.customerSinkFunc = (CustomerSinkFunc) elasticsearchSinkFunction;
        this.userConfig = userConfig;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        initMetric();
    }

    public void initMetric() {
        Counter counter = getRuntimeContext().getMetricGroup().counter(MetricConstant.DT_NUM_RECORDS_OUT);
        Counter outDirtyRecords = getRuntimeContext().getMetricGroup().counter(MetricConstant.DT_NUM_DIRTY_RECORDS_OUT);

        customerSinkFunc.setOutRecords(counter);
        customerSinkFunc.setOutDirtyRecords(outDirtyRecords);
        outRecordsRate = getRuntimeContext().getMetricGroup().meter(MetricConstant.DT_NUM_RECORDS_OUT_RATE, new MeterView(counter, 20));
    }
}
