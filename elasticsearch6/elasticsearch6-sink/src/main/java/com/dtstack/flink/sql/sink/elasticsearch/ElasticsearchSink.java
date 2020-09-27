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

import com.dtstack.flink.sql.table.AbstractTargetTableInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

import com.dtstack.flink.sql.sink.IStreamSinkGener;
import com.dtstack.flink.sql.sink.elasticsearch.table.ElasticsearchTableInfo;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpHost;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author yinxi
 * @date 2020/1/9 - 15:08
 */
public class ElasticsearchSink implements RetractStreamTableSink<Row>, IStreamSinkGener<ElasticsearchSink> {

    private final int ES_DEFAULT_PORT = 9200;
    private final String ES_DEFAULT_SCHEMA = "http";

    private String clusterName;

    private int bulkFlushMaxActions = 1;

    private List<String> esAddressList;

    private String index = "";

    private String type = "";

    private List<Integer> idIndexList;

    protected String[] fieldNames;

    protected String[] columnTypes;

    private TypeInformation[] fieldTypes;

    private int parallelism = -1;

    private ElasticsearchTableInfo esTableInfo;


    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        return this;
    }

    @Override
    public TupleTypeInfo<Tuple2<Boolean, Row>> getOutputType() {
        return new TupleTypeInfo(org.apache.flink.table.api.Types.BOOLEAN(), getRecordType());
    }

    @Override
    public TypeInformation<Row> getRecordType() {
        return new RowTypeInfo(fieldTypes, fieldNames);
    }

    @Override
    public String[] getFieldNames() {
        return fieldNames;
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return fieldTypes;
    }


    private RichSinkFunction createEsSinkFunction() {
        Map<String, String> userConfig = Maps.newHashMap();
        userConfig.put("cluster.name", clusterName);
        // This instructs the sink to emit after every element, otherwise they would be buffered
        userConfig.put(org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, "" + bulkFlushMaxActions);

        List<HttpHost> transports = esAddressList.stream()
                .map(address -> address.split(":"))
                .map(addressArray -> {
                    String host = addressArray[0].trim();
                    int port = addressArray.length > 1 ? Integer.valueOf(addressArray[1].trim()) : ES_DEFAULT_PORT;
                    return new HttpHost(host.trim(), port, ES_DEFAULT_SCHEMA);
                }).collect(Collectors.toList());

        CustomerSinkFunc customerSinkFunc = new CustomerSinkFunc(index, type, Arrays.asList(fieldNames), Arrays.asList(columnTypes), idIndexList);
        return new MetricElasticsearch6Sink(userConfig, transports, customerSinkFunc, esTableInfo);
    }

    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        RichSinkFunction richSinkFunction = createEsSinkFunction();
        DataStreamSink streamSink = dataStream.addSink(richSinkFunction);
        if (parallelism > 0) {
            streamSink.setParallelism(parallelism);
        }
    }

    @Override
    public ElasticsearchSink genStreamSink(AbstractTargetTableInfo targetTableInfo) {
        esTableInfo = (ElasticsearchTableInfo) targetTableInfo;
        clusterName = esTableInfo.getClusterName();
        index = esTableInfo.getIndex();
        type = esTableInfo.getEsType();
        columnTypes = esTableInfo.getFieldTypes();
        esAddressList = Arrays.asList(esTableInfo.getAddress().split(","));
        this.bulkFlushMaxActions = esTableInfo.getBatchSize();
        String id = esTableInfo.getId();

        if (!StringUtils.isEmpty(id)) {
            idIndexList = Arrays.stream(StringUtils.split(id, ",")).map(Integer::valueOf).collect(Collectors.toList());
        }
        return this;
    }
}
