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

package com.dtstack.flink.sql.side.elasticsearch6.table;

import com.dtstack.flink.sql.enums.ECacheContentType;
import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.CacheMissVal;
import com.dtstack.flink.sql.side.PredicateInfo;
import com.dtstack.flink.sql.side.cache.CacheObj;
import com.dtstack.flink.sql.side.elasticsearch6.Elasticsearch6AsyncSideInfo;
import com.dtstack.flink.sql.side.elasticsearch6.util.Es6Util;
import com.dtstack.flink.sql.side.table.BaseAsyncTableFunction;
import com.dtstack.flink.sql.util.SwitchUtil;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.dtstack.flink.sql.side.elasticsearch6.util.Es6Util.removeSpaceAndApostrophe;
import static com.dtstack.flink.sql.side.elasticsearch6.util.Es6Util.textCastToKeyword;

/**
 * @author: chuixue
 * @create: 2020-11-17 13:58
 * @description:
 **/
public class Elasticsearch6AsyncTableFunction extends BaseAsyncTableFunction {

    private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch6AsyncTableFunction.class);
    private transient RestHighLevelClient rhlClient;
    private SearchRequest searchRequest;
    private AbstractSideTableInfo sideTableInfo = sideInfo.getSideTableInfo();
    private Map<String, String> physicalFields = sideTableInfo.getPhysicalFields();

    public Elasticsearch6AsyncTableFunction(AbstractSideTableInfo sideTableInfo, String[] lookupKeys) {
        super(new Elasticsearch6AsyncSideInfo(sideTableInfo, lookupKeys));
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        Elasticsearch6SideTableInfo tableInfo = (Elasticsearch6SideTableInfo) sideInfo.getSideTableInfo();
        rhlClient = Es6Util.getClient(tableInfo.getAddress(), tableInfo.isAuthMesh(), tableInfo.getUserName(), tableInfo.getPassword());
        searchRequest = Es6Util.setSearchRequest(sideInfo);
    }

    @Override
    public void handleAsyncInvoke(CompletableFuture<Collection<Row>> future, Object... keys) throws Exception {
        String key = buildCacheKey(keys);
        BoolQueryBuilder boolQueryBuilder = Es6Util.setPredicateclause(sideInfo);
        boolQueryBuilder = setInputParams(boolQueryBuilder, keys);
        SearchSourceBuilder searchSourceBuilder = initConfiguration();
        searchSourceBuilder.query(boolQueryBuilder);
        searchRequest.source(searchSourceBuilder);

        // 异步查询数据
        rhlClient.searchAsync(searchRequest, RequestOptions.DEFAULT, new ActionListener<SearchResponse>() {

            //成功响应时
            @Override
            public void onResponse(SearchResponse searchResponse) {

                List<Object> cacheContent = Lists.newArrayList();
                List<Row> rowList = Lists.newArrayList();
                SearchHit[] searchHits = searchResponse.getHits().getHits();
                if (searchHits.length > 0) {
                    Elasticsearch6SideTableInfo tableInfo = null;
                    RestHighLevelClient tmpRhlClient = null;
                    try {
                        while (true) {
                            loadDataToCache(searchHits, rowList, cacheContent);
                            // determine if all results haven been ferched
                            if (searchHits.length < getFetchSize()) {
                                break;
                            }
                            if (tableInfo == null) {
                                // create new connection to fetch data
                                tableInfo = (Elasticsearch6SideTableInfo) sideInfo.getSideTableInfo();
                                tmpRhlClient = Es6Util.getClient(tableInfo.getAddress(), tableInfo.isAuthMesh(), tableInfo.getUserName(), tableInfo.getPassword());
                            }
                            Object[] searchAfterParameter = searchHits[searchHits.length - 1].getSortValues();
                            searchSourceBuilder.searchAfter(searchAfterParameter);
                            searchRequest.source(searchSourceBuilder);
                            searchResponse = tmpRhlClient.search(searchRequest, RequestOptions.DEFAULT);
                            searchHits = searchResponse.getHits().getHits();
                        }
                        dealCacheData(key, CacheObj.buildCacheObj(ECacheContentType.MultiLine, cacheContent));
                        future.complete(rowList);
                    } catch (Exception e) {
                        dealFillDataError(future, e);
                    } finally {
                        if (tmpRhlClient != null) {
                            try {
                                tmpRhlClient.close();
                            } catch (IOException e) {
                                LOG.warn("Failed to shut down tmpRhlClient.", e);
                            }
                        }
                    }
                } else {
                    dealMissKey(future);
                    dealCacheData(key, CacheMissVal.getMissKeyObj());
                }
            }

            // 响应失败处理
            @Override
            public void onFailure(Exception e) {
                LOG.error("" + e);
                future.completeExceptionally(new RuntimeException("Response failed!"));
            }
        });
    }

    private void loadDataToCache(SearchHit[] searchHits, List<Row> rowList, List<Object> cacheContent) {
        List<Object> results = Lists.newArrayList();
        for (SearchHit searchHit : searchHits) {
            Map<String, Object> object = searchHit.getSourceAsMap();
            results.add(object);
        }
        rowList.addAll(getRows(cacheContent, results));
    }

    protected List<Row> getRows(List<Object> cacheContent, List<Object> results) {
        List<Row> rowList = Lists.newArrayList();
        for (Object line : results) {
            Row row = fillData(line);
            if (null != cacheContent && openCache()) {
                cacheContent.add(line);
            }
            rowList.add(row);
        }
        return rowList;
    }

    @Override
    public Row fillData(Object sideInput) {
        Map<String, Object> values = (Map<String, Object>) sideInput;
        Row row = new Row(physicalFields.size());
        if (sideInput != null) {
            String[] sideFieldNames = physicalFields.values().stream().toArray(String[]::new);
            String[] sideFieldTypes = sideTableInfo.getFieldTypes();
            for (int i = 0; i < sideFieldNames.length; i++) {
                row.setField(i, SwitchUtil.getTarget(values.get(sideFieldNames[i].trim()), sideFieldTypes[i]));
            }
        }
        row.setKind(RowKind.INSERT);
        return row;
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (rhlClient != null) {
            rhlClient.close();
        }
    }

    private SearchSourceBuilder initConfiguration() {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(getFetchSize());
        searchSourceBuilder.sort("_id", SortOrder.DESC);
        List<String> collect = Stream
                .of(sideTableInfo.getFields())
                .map(e -> physicalFields.getOrDefault(e, e))
                .collect(Collectors.toList());
        String[] sideFieldNames = collect.toArray(new String[collect.size()]);
        searchSourceBuilder.fetchSource(sideFieldNames, null);

        return searchSourceBuilder;
    }

    private BoolQueryBuilder setInputParams(BoolQueryBuilder boolQueryBuilder, Object... keys) {
        if (boolQueryBuilder == null) {
            boolQueryBuilder = new BoolQueryBuilder();
        }
        // lookup join支持常量和非等职join，前提条件是：第一个join条件必须是等值关联维表字段
        String[] lookupKeys = sideInfo.getLookupKeys();
        for (int i = 0; i < lookupKeys.length; i++) {
            String fieldName = lookupKeys[i];
            String operatorKind = "=";
            String condition = keys[i] + "";

            PredicateInfo info = new PredicateInfo(null, operatorKind, null, fieldName, condition);
            if (!StringUtils.isBlank(info.getCondition())) {
                boolQueryBuilder = boolQueryBuilder.must(QueryBuilders.termQuery(textCastToKeyword(info.getFieldName(), sideInfo), removeSpaceAndApostrophe(info.getCondition())[0]));
            }
        }
        return boolQueryBuilder;
    }
}
