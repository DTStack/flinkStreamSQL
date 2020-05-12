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
import com.dtstack.flink.sql.side.BaseAsyncReqRow;
import com.dtstack.flink.sql.side.CacheMissVal;
import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.PredicateInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.types.Row;

import com.dtstack.flink.sql.enums.ECacheContentType;
import com.dtstack.flink.sql.side.cache.CacheObj;
import com.dtstack.flink.sql.side.elasticsearch6.table.Elasticsearch6SideTableInfo;
import com.dtstack.flink.sql.side.elasticsearch6.util.Es6Util;
import com.dtstack.flink.sql.side.elasticsearch6.util.SwitchUtil;
import com.dtstack.flink.sql.util.ParseUtils;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.ActionListener;
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
import java.util.List;
import java.util.Map;

/**
 * @author yinxi
 * @date 2020/2/13 - 13:10
 */
public class Elasticsearch6AsyncReqRow extends BaseAsyncReqRow implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch6AsyncReqRow.class);
    private transient RestHighLevelClient rhlClient;
    private SearchRequest searchRequest;
    private List<String> sqlJoinCompareOperate = Lists.newArrayList();

    public Elasticsearch6AsyncReqRow(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, AbstractSideTableInfo sideTableInfo) {
        super(new Elasticsearch6AsyncSideInfo(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo));
        SqlNode conditionNode = joinInfo.getCondition();
        ParseUtils.parseJoinCompareOperate(conditionNode, sqlJoinCompareOperate);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Elasticsearch6SideTableInfo tableInfo = (Elasticsearch6SideTableInfo) sideInfo.getSideTableInfo();
        rhlClient = Es6Util.getClient(tableInfo.getAddress(), tableInfo.isAuthMesh(), tableInfo.getUserName(), tableInfo.getPassword());
        searchRequest = Es6Util.setSearchRequest(sideInfo);

    }


    @Override
    public void handleAsyncInvoke(Map<String, Object> inputParams, CRow input, ResultFuture<CRow> resultFuture) throws Exception {
        String key = buildCacheKey(inputParams);
        BoolQueryBuilder boolQueryBuilder = Es6Util.setPredicateclause(sideInfo);
        boolQueryBuilder = setInputParams(inputParams, boolQueryBuilder);
        SearchSourceBuilder searchSourceBuilder = initConfiguration();
        searchSourceBuilder.query(boolQueryBuilder);
        searchRequest.source(searchSourceBuilder);

        // 异步查询数据
        rhlClient.searchAsync(searchRequest, RequestOptions.DEFAULT, new ActionListener<SearchResponse>() {

            //成功响应时
            @Override
            public void onResponse(SearchResponse searchResponse) {

                List<Object> cacheContent = Lists.newArrayList();
                List<CRow> rowList = Lists.newArrayList();
                SearchHit[] searchHits = searchResponse.getHits().getHits();
                if (searchHits.length > 0) {
                    Elasticsearch6SideTableInfo tableInfo = null;
                    RestHighLevelClient tmpRhlClient = null;
                    try {
                        while (true) {
                            loadDataToCache(searchHits, rowList, cacheContent, input);
                            // determine if all results haven been ferched
                            if (searchHits.length < getFetchSize()) {
                                break;
                            }
                            if (tableInfo == null && tmpRhlClient == null) {
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
                        resultFuture.complete(rowList);
                    } catch (Exception e) {
                        dealFillDataError(resultFuture, e, input);
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
                    dealMissKey(input, resultFuture);
                    dealCacheData(key, CacheMissVal.getMissKeyObj());
                }
            }

            // 响应失败处理
            @Override
            public void onFailure(Exception e) {
                LOG.error("" + e);
                resultFuture.completeExceptionally(new RuntimeException("Response failed!"));
            }
        });
    }

    @Override
    public String buildCacheKey(Map<String, Object> inputParams) {
        StringBuilder sb = new StringBuilder();
        for (Object ele : inputParams.values()) {
            sb.append(ele.toString())
                    .append("_");
        }

        return sb.toString();
    }

    private void loadDataToCache(SearchHit[] searchHits, List<CRow> rowList, List<Object> cacheContent, CRow copyCrow) {
        List<Object> results = Lists.newArrayList();
        for (SearchHit searchHit : searchHits) {
            Map<String, Object> object = searchHit.getSourceAsMap();
            results.add(object);
        }
        rowList.addAll(getRows(copyCrow, cacheContent, results));
    }

    protected List<CRow> getRows(CRow inputRow, List<Object> cacheContent, List<Object> results) {
        List<CRow> rowList = Lists.newArrayList();
        for (Object line : results) {
            Row row = fillData(inputRow.row(), line);
            if (null != cacheContent && openCache()) {
                cacheContent.add(line);
            }
            rowList.add(new CRow(row, inputRow.change()));
        }
        return rowList;
    }

    @Override
    public Row fillData(Row input, Object line) {

        Map<String, Object> cacheInfo = (Map<String, Object>) line;
        Row row = new Row(sideInfo.getOutFieldInfoList().size());
        String[] fields = sideInfo.getSideTableInfo().getFieldTypes();
        for (Map.Entry<Integer, Integer> entry : sideInfo.getInFieldIndex().entrySet()) {
            Object obj = input.getField(entry.getValue());
            boolean isTimeIndicatorTypeInfo = TimeIndicatorTypeInfo.class.isAssignableFrom(sideInfo.getRowTypeInfo().getTypeAt(entry.getValue()).getClass());
            if (obj instanceof Timestamp && isTimeIndicatorTypeInfo) {
                obj = ((Timestamp) obj).getTime();
            }

            row.setField(entry.getKey(), obj);
        }

        for (Map.Entry<Integer, Integer> entry : sideInfo.getSideFieldIndex().entrySet()) {
            if (cacheInfo == null) {
                row.setField(entry.getKey(), null);
            } else {
                Object object = SwitchUtil.getTarget(cacheInfo.get(sideInfo.getSideFieldNameIndex().get(entry.getKey())), fields[entry.getValue()]);
                row.setField(entry.getKey(), object);
            }
        }

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
        String[] sideFieldNames = StringUtils.split(sideInfo.getSideSelectFields().trim(), ",");
        searchSourceBuilder.fetchSource(sideFieldNames, null);

        return searchSourceBuilder;
    }

    private BoolQueryBuilder setInputParams(Map<String, Object> inputParams, BoolQueryBuilder boolQueryBuilder) {
        if (boolQueryBuilder == null) {
            boolQueryBuilder = new BoolQueryBuilder();
        }

        for (int i = 0; i < sqlJoinCompareOperate.size(); i++) {
            String fieldName = sideInfo.getEqualFieldList().get(i);
            String operatorKind = sqlJoinCompareOperate.get(sideInfo.getEqualFieldList().indexOf(fieldName));
            String condition = String.valueOf(inputParams.get(fieldName));
            boolQueryBuilder = Es6Util.buildFilterCondition(boolQueryBuilder, new PredicateInfo(null, operatorKind, null, fieldName, condition), sideInfo);
        }

        return boolQueryBuilder;
    }

    public int getFetchSize() {
        return 1000;
    }
}

