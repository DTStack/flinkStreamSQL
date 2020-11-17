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

import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.elasticsearch6.Elasticsearch6AllSideInfo;
import com.dtstack.flink.sql.side.elasticsearch6.util.Es6Util;
import com.dtstack.flink.sql.side.table.BaseTableFunction;
import com.dtstack.flink.sql.util.SwitchUtil;
import com.google.common.collect.Maps;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author: chuixue
 * @create: 2020-11-17 09:35
 * @description:
 **/
public class Elasticsearch6TableFunction extends BaseTableFunction {
    private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch6TableFunction.class);

    private transient RestHighLevelClient rhlClient;
    private SearchRequest searchRequest;
    private BoolQueryBuilder boolQueryBuilder;
    private AbstractSideTableInfo sideTableInfo = sideInfo.getSideTableInfo();
    private Map<String, String> physicalFields = sideTableInfo.getPhysicalFields();

    public Elasticsearch6TableFunction(AbstractSideTableInfo sideTableInfo, String[] lookupKeys) {
        super(new Elasticsearch6AllSideInfo(sideTableInfo, lookupKeys));
    }

    @Override
    protected void initCache() throws SQLException {
        Map<String, List<Map<String, Object>>> newCache = Maps.newConcurrentMap();
        cacheRef.set(newCache);
        try {
            // create search request and build where cause
            searchRequest = Es6Util.setSearchRequest(sideInfo);
            boolQueryBuilder = Es6Util.setPredicateclause(sideInfo);
            loadData(newCache);
        } catch (Exception e) {
            LOG.error("", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void reloadCache() {
        //reload cacheRef and replace to old cacheRef
        Map<String, List<Map<String, Object>>> newCache = Maps.newConcurrentMap();
        try {
            loadData(newCache);
        } catch (Exception e) {
            LOG.error("", e);
            throw new RuntimeException(e);
        }

        cacheRef.set(newCache);
        LOG.info("----- elasticsearch6 all cacheRef reload end:{}", Calendar.getInstance());

    }

    private void loadData(Map<String, List<Map<String, Object>>> tmpCache) throws IOException {
        Elasticsearch6SideTableInfo tableInfo = (Elasticsearch6SideTableInfo) sideTableInfo;

        try {
            for (int i = 0; i < CONN_RETRY_NUM; i++) {
                try {
                    rhlClient = Es6Util.getClient(tableInfo.getAddress(), tableInfo.isAuthMesh(), tableInfo.getUserName(), tableInfo.getPassword());
                    break;
                } catch (Exception e) {
                    if (i == CONN_RETRY_NUM - 1) {
                        throw new RuntimeException("", e);
                    }

                    try {
                        String connInfo = "url: " + tableInfo.getAddress() + "; userName: " + tableInfo.getUserName() + ", pwd:" + tableInfo.getPassword();
                        LOG.warn("get conn fail, wait for 5 sec and try again, connInfo:" + connInfo);
                        Thread.sleep(LOAD_DATA_ERROR_SLEEP_TIME);
                    } catch (InterruptedException e1) {
                        LOG.error("", e1);
                    }
                }

            }
            SearchSourceBuilder searchSourceBuilder = initConfiguration(boolQueryBuilder);
            searchData(searchSourceBuilder, tmpCache);

        } catch (Exception e) {
            LOG.error("", e);
            throw new RuntimeException(e);
        } finally {

            if (rhlClient != null) {
                rhlClient.close();
            }
        }
    }

    // initialize searchSourceBuilder
    private SearchSourceBuilder initConfiguration(BoolQueryBuilder boolQueryBuilder) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        if (boolQueryBuilder != null) {
            searchSourceBuilder.query(boolQueryBuilder);
        }

        searchSourceBuilder.size(getFetchSize());
        searchSourceBuilder.sort("_id", SortOrder.DESC);

        // fields included in the source data
        List<String> collect = Stream
                .of(sideTableInfo.getFields())
                .map(e -> physicalFields.getOrDefault(e, e))
                .collect(Collectors.toList());
        String[] sideFieldNames = collect.toArray(new String[collect.size()]);
        searchSourceBuilder.fetchSource(sideFieldNames, null);
        return searchSourceBuilder;
    }


    private void searchData(SearchSourceBuilder searchSourceBuilder, Map<String, List<Map<String, Object>>> tmpCache) {

        Object[] searchAfterParameter = null;
        SearchResponse searchResponse = null;
        SearchHit[] searchHits = null;

        while (true) {
            try {
                if (searchAfterParameter != null) {
                    // set search mark
                    searchSourceBuilder.searchAfter(searchAfterParameter);
                }

                searchRequest.source(searchSourceBuilder);
                searchResponse = rhlClient.search(searchRequest, RequestOptions.DEFAULT);
                searchHits = searchResponse.getHits().getHits();
                loadToCache(searchHits, tmpCache);

                if (searchHits.length < getFetchSize()) {
                    break;
                }

                searchAfterParameter = searchHits[searchHits.length - 1].getSortValues();
            } catch (IOException e) {
                LOG.error("Query failed!", e);
            }
        }
    }

    // data load to cache
    private void loadToCache(SearchHit[] searchHits, Map<String, List<Map<String, Object>>> tmpCache) {
        String[] sideFieldNames = physicalFields.values().stream().toArray(String[]::new);
        String[] sideFieldTypes = sideTableInfo.getFieldTypes();

        for (SearchHit searchHit : searchHits) {
            Map<String, Object> oneRow = Maps.newHashMap();
            for (int i = 0; i < sideFieldNames.length; i++) {
                Object object = searchHit.getSourceAsMap().get(sideFieldNames[i].trim());
                object = SwitchUtil.getTarget(object, sideFieldTypes[i]);
                oneRow.put(sideFieldNames[i].trim(), object);
            }

            buildCache(oneRow, tmpCache);
        }
    }
}
