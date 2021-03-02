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

import com.dtstack.flink.sql.util.DateUtil;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch.index.IndexGenerator;
import org.apache.flink.streaming.connectors.elasticsearch.index.IndexGeneratorFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.lucene.analysis.da.DanishAnalyzer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * @description:
 * @program: flink.sql
 * @author: lany
 * @create: 2021/01/04 17:19
 */
public class CustomerSinkFunc implements ElasticsearchSinkFunction<Tuple2> {

    private final Logger logger = LoggerFactory.getLogger(CustomerSinkFunc.class);
    /**
     * 用作ID的属性值连接符号
     */
    private static final String ID_VALUE_SPLIT = "_";

    private String index_definition;

    private List<String> idFiledNames;

    private List<String> fieldNames;

    private List<String> fieldTypes;

    private IndexGenerator indexGenerator;

    private transient Counter outRecords;

    private transient Counter outDirtyRecords;

    private RestHighLevelClient rhlClient;

    // cache dynamic index and add expire time for index
    private transient Cache<String, String> dynamicIndexCache;

    public CustomerSinkFunc(String index, String index_definition, List<String> fieldNames, List<String> fieldTypes, TypeInformation<?>[] typeInformatics, List<String> idFiledNames) {
        this.index_definition = index_definition;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.idFiledNames = idFiledNames;
        this.indexGenerator = IndexGeneratorFactory.createIndexGenerator(index,(String[]) fieldNames.toArray(), typeInformatics);
    }

    @Override
    public void process(Tuple2 tuple2, RuntimeContext ctx, RequestIndexer indexer) {
        try {
            Tuple2<Boolean, Row> tupleTrans = tuple2;
            Boolean retract = tupleTrans.getField(0);
            Row element = tupleTrans.getField(1);
            if (!retract) {
                return;
            }

            indexer.add(createIndexRequest(element));
            outRecords.inc();
        } catch (RuntimeException e) {
            throw new SuppressRestartsException(e);
        } catch (Throwable e) {
            outDirtyRecords.inc();
            logger.error("Failed to store source data {}. ", tuple2.getField(1));
            logger.error("Failed to create index request exception. ", e);
        }
    }

    public void setOutRecords(Counter outRecords) {
        this.outRecords = outRecords;
    }

    public void setOutDirtyRecords(Counter outDirtyRecords) {
        this.outDirtyRecords = outDirtyRecords;
    }

    public void setRhlClient(RestHighLevelClient rhlClient) {
        this.rhlClient = rhlClient;
    }

    public void getDynamicIndexCache() {
        this.dynamicIndexCache = CacheBuilder.newBuilder()
                .removalListener((RemovalListener<String, String>) notification -> logger.warn("Index [{}] has been remove from cache because expired.", notification.getKey()))
                .expireAfterAccess(10, TimeUnit.MINUTES)
                .build();
    }

    public IndexGenerator getIndexGenerator() {
        return indexGenerator;
    }

    private IndexRequest createIndexRequest(Row element) {

        Map<String, Object> dataMap = Es7Util.rowToJsonMap(element, fieldNames, fieldTypes);
        int length = Math.min(element.getArity(), fieldNames.size());
        for (int i = 0; i < length; i++) {
            if (element.getField(i) instanceof Date) {
                dataMap.put(fieldNames.get(i), DateUtil.transformSqlDateToUtilDate((Date) element.getField(i)));
                continue;
            }
            if (element.getField(i) instanceof Timestamp) {
                dataMap.put(fieldNames.get(i), ((Timestamp) element.getField(i)).getTime());
                continue;
            }
            dataMap.put(fieldNames.get(i), element.getField(i));
        }

        String index = indexGenerator.generate(element);
        try {
            if (null == dynamicIndexCache.getIfPresent(index)
                    && !rhlClient.indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT)
                    && index_definition != null) {
                CreateIndexRequest indexRequest = new CreateIndexRequest(index).source(index_definition, XContentType.JSON);
                rhlClient.indices().create(indexRequest, RequestOptions.DEFAULT);
                logger.info(String.format("Dynamic index : {%s} is created.",index));
                dynamicIndexCache.put(index, index);
            }
        } catch (Exception e) {
            throw new RuntimeException(String.format("Create index {%s} failed.The reason is {}", index, e.getMessage()));
        }


        String idFieldStr = "";
        if (null != idFiledNames) {
            idFieldStr = idFiledNames.stream()
                    .map(idFiledName -> dataMap.get(idFiledName).toString())
                    .collect(Collectors.joining(ID_VALUE_SPLIT));
        }

        if (StringUtils.isEmpty(idFieldStr)) {
            return Requests.indexRequest()
                    .index(index)
                    .source(dataMap);
        }

        return Requests.indexRequest()
                .index(index)
                .id(idFieldStr)
                .source(dataMap);
    }

}
