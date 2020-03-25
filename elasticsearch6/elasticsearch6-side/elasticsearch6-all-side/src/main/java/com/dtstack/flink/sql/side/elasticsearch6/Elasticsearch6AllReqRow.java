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

package com.dtstack.flink.sql.side.elasticsearch6;

import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.BaseAllReqRow;
import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.elasticsearch6.table.Elasticsearch6SideTableInfo;
import com.dtstack.flink.sql.side.elasticsearch6.util.Es6Util;
import com.dtstack.flink.sql.side.elasticsearch6.util.SwitchUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
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
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author yinxi
 * @date 2020/1/13 - 1:00
 */
public class Elasticsearch6AllReqRow extends BaseAllReqRow implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch6AllReqRow.class);

    private static final int CONN_RETRY_NUM = 3;
    private AtomicReference<Map<String, List<Map<String, Object>>>> cacheRef = new AtomicReference<>();
    private transient RestHighLevelClient rhlClient;
    private SearchRequest searchRequest;
    private BoolQueryBuilder boolQueryBuilder;

    public Elasticsearch6AllReqRow(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, AbstractSideTableInfo sideTableInfo) {
        super(new Elasticsearch6AllSideInfo(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo));
    }

    @Override
    public void flatMap(Tuple2<Boolean,Row> value, Collector<Tuple2<Boolean,Row>> out) throws Exception {
        List<Object> inputParams = Lists.newArrayList();
        for (Integer conValIndex : sideInfo.getEqualValIndex()) {
            Object equalObj = value.f1.getField(conValIndex);
            if (equalObj == null) {
                sendOutputRow(value, null, out);
                return;
            }

            inputParams.add(equalObj);
        }

        String key = buildKey(inputParams);
        List<Map<String, Object>> cacheList = cacheRef.get().get(key);
        if (CollectionUtils.isEmpty(cacheList)) {
            sendOutputRow(value, null, out);
            return;
        }

        for (Map<String, Object> one : cacheList) {
            sendOutputRow(value, one, out);
        }
    }

    @Override
    public Row fillData(Row input, Object sideInput) {
        Map<String, Object> cacheInfo = (Map<String, Object>) sideInput;
        Row row = new Row(sideInfo.getOutFieldInfoList().size());
        for (Map.Entry<Integer, Integer> entry : sideInfo.getInFieldIndex().entrySet()) {
            Object obj = input.getField(entry.getValue());
            boolean isTimeIndicatorTypeInfo = TimeIndicatorTypeInfo.class.isAssignableFrom(sideInfo.getRowTypeInfo().getTypeAt(entry.getValue()).getClass());

            //Type information for indicating event or processing time. However, it behaves like a regular SQL timestamp but is serialized as Long.
            if (obj instanceof Timestamp && isTimeIndicatorTypeInfo) {
                obj = ((Timestamp) obj).getTime();
            }

            row.setField(entry.getKey(), obj);
        }

        for (Map.Entry<Integer, String> entry : sideInfo.getSideFieldNameIndex().entrySet()) {
            if (cacheInfo == null) {
                row.setField(entry.getKey(), null);
            } else {
                row.setField(entry.getKey(), cacheInfo.get(entry.getValue()));
            }
        }

        return row;
    }

    private String buildKey(List<Object> equalValList) {
        StringBuilder sb = new StringBuilder("");
        for (Object equalVal : equalValList) {
            sb.append(equalVal).append("_");
        }

        return sb.toString();
    }

    private String buildKey(Map<String, Object> val, List<String> equalFieldList) {
        StringBuilder sb = new StringBuilder("");
        for (String equalField : equalFieldList) {
            sb.append(val.get(equalField)).append("_");
        }

        return sb.toString();
    }

    @Override
    protected void initCache() {
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
        Elasticsearch6SideTableInfo tableInfo = (Elasticsearch6SideTableInfo) sideInfo.getSideTableInfo();

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
                        Thread.sleep(5 * 1000);
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
    private SearchSourceBuilder initConfiguration(BoolQueryBuilder boolQueryBuilder){
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        if (boolQueryBuilder != null) {
            searchSourceBuilder.query(boolQueryBuilder);
        }

        searchSourceBuilder.size(getFetchSize());
        searchSourceBuilder.sort("_id", SortOrder.DESC);

        // fields included in the source data
        String[] sideFieldNames = StringUtils.split(sideInfo.getSideSelectFields().trim(), ",");
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
        String[] sideFieldNames = StringUtils.split(sideInfo.getSideSelectFields().trim(), ",");
        String[] sideFieldTypes = sideInfo.getSideTableInfo().getFieldTypes();

        for (SearchHit searchHit : searchHits) {
            Map<String, Object> oneRow = Maps.newHashMap();
            for (String fieldName : sideFieldNames) {
                Object object = searchHit.getSourceAsMap().get(fieldName.trim());
                int fieldIndex = sideInfo.getSideTableInfo().getFieldList().indexOf(fieldName.trim());
                object = SwitchUtil.getTarget(object, sideFieldTypes[fieldIndex]);
                oneRow.put(fieldName.trim(), object);
            }

            String cacheKey = buildKey(oneRow, sideInfo.getEqualFieldList());
            List<Map<String, Object>> list = tmpCache.computeIfAbsent(cacheKey, key -> Lists.newArrayList());
            list.add(oneRow);

        }
    }

    public int getFetchSize() {
        return 1000;
    }
}
