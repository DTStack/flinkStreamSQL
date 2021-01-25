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
import com.google.common.base.Preconditions;
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
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * table output elastic5plugin
 * Date: 2018/7/13
 * Company: www.dtstack.com
 * @author huyifan_zju@163.com
 */

public class ElasticsearchSink implements RetractStreamTableSink<Row>, IStreamSinkGener<ElasticsearchSink> {

    private final Logger logger = LoggerFactory.getLogger(ElasticsearchSink.class);

    private String clusterName;

    private int bulkFlushMaxActions = 1;

    private List<String> esAddressList;

    private String index = "";

    private String type = "";

    private List<String> idFiledNames;

    protected String[] fieldNames;

    protected String[] columnTypes;

    private TypeInformation[] fieldTypes;

    private int parallelism = 1;

    protected String registerTableName;

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


    private RichSinkFunction createEsSinkFunction(){

        // check whether id fields is exists in columns
        List<String> filedNamesLists = Arrays.asList(fieldNames);
        Preconditions.checkState(filedNamesLists.containsAll(idFiledNames), "elasticsearch5 type of id %s is should be exists in columns %s.", idFiledNames, filedNamesLists);
        CustomerSinkFunc customerSinkFunc = new CustomerSinkFunc(index, type, filedNamesLists, Arrays.asList(columnTypes), idFiledNames);

        Map<String, String> userConfig = new HashMap<>();
        userConfig.put("cluster.name", clusterName);
        // This instructs the sink to emit after every element, otherwise they would be buffered
        userConfig.put(org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, "" + bulkFlushMaxActions);
        List<InetSocketAddress> transports = new ArrayList<>();

        for(String address : esAddressList){
            String[] infoArray = StringUtils.split(address, ":");
            int port = 9300;
            String host = infoArray[0];
            if(infoArray.length > 1){
                port = Integer.valueOf(infoArray[1].trim());
            }

            try {
                transports.add(new InetSocketAddress(InetAddress.getByName(host), port));
            }catch (Exception e){
                logger.error("", e);
                throw new RuntimeException(e);
            }
        }

        boolean authMesh = esTableInfo.isAuthMesh();
        if (authMesh) {
            String authPassword = esTableInfo.getUserName() + ":" + esTableInfo.getPassword();
            userConfig.put("xpack.security.user", authPassword);
        }

        return new MetricElasticsearchSink(userConfig, transports, customerSinkFunc, esTableInfo);
    }

    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        consumeDataStream(dataStream);
    }

    @Override
    public DataStreamSink<Tuple2<Boolean, Row>> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        RichSinkFunction richSinkFunction = createEsSinkFunction();
        DataStreamSink streamSink = dataStream.addSink(richSinkFunction).name(registerTableName);
        if(parallelism > 0){
            streamSink.setParallelism(parallelism);
        }

        return streamSink;
    }

    public void setBulkFlushMaxActions(int bulkFlushMaxActions) {
        this.bulkFlushMaxActions = bulkFlushMaxActions;
    }

    @Override
    public ElasticsearchSink genStreamSink(AbstractTargetTableInfo targetTableInfo) {
        ElasticsearchTableInfo elasticsearchTableInfo = (ElasticsearchTableInfo) targetTableInfo;
        esTableInfo = elasticsearchTableInfo;
        clusterName = elasticsearchTableInfo.getClusterName();
        String address = elasticsearchTableInfo.getAddress();
        String[] addr = StringUtils.split(address, ",");
        esAddressList = Arrays.asList(addr);
        index = elasticsearchTableInfo.getIndex();
        type = elasticsearchTableInfo.getEsType();
        String id = elasticsearchTableInfo.getId();
        String[] idField = StringUtils.split(id, ",");
        idFiledNames = new ArrayList<>();
        registerTableName = elasticsearchTableInfo.getName();
        parallelism = Objects.isNull(elasticsearchTableInfo.getParallelism()) ?
                parallelism : elasticsearchTableInfo.getParallelism();

        for(int i = 0; i < idField.length; ++i) {
            idFiledNames.add(String.valueOf(idField[i]));
        }

        columnTypes = elasticsearchTableInfo.getFieldTypes();

        return this;
    }
}
