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


package com.dtstack.flink.sql.side.elasticsearch7;

import com.dtstack.flink.sql.enums.ECacheContentType;
import com.dtstack.flink.sql.side.*;
import com.dtstack.flink.sql.side.cache.CacheObj;
import com.dtstack.flink.sql.side.elasticsearch7.table.Elasticsearch7SideTableInfo;
import com.dtstack.flink.sql.side.elasticsearch7.util.Es7Util;
import com.dtstack.flink.sql.util.ParseUtils;
import com.dtstack.flink.sql.util.RowDataComplete;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @description:
 * @program: flink.sql
 * @author: lany
 * @create: 2021/01/11 11:21
 */
public class Elasticsearch7AsyncReqRow extends BaseAsyncReqRow implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch7AsyncReqRow.class);
    private transient RestHighLevelClient rhlClient;
    private SearchRequest searchRequest;
    private List<String> sqlJoinCompareOperate = new ArrayList<>();

    public Elasticsearch7AsyncReqRow(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, AbstractSideTableInfo sideTableInfo) {
        super(new Elasticsearch7AsyncSideInfo(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo));
        SqlNode conditionNode = joinInfo.getCondition();
        ParseUtils.parseJoinCompareOperate(conditionNode, sqlJoinCompareOperate);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Elasticsearch7SideTableInfo tableInfo = (Elasticsearch7SideTableInfo) sideInfo.getSideTableInfo();
        rhlClient = Es7Util.getClient(tableInfo.getAddress(),
                tableInfo.isAuthMesh(),
                tableInfo.getUserName(),
                tableInfo.getPassword());
        searchRequest = Es7Util.setSearchRequest(sideInfo);
    }

    @Override
    public void handleAsyncInvoke(Map<String, Object> inputParams, BaseRow input, ResultFuture<BaseRow> resultFuture) throws Exception {
        String key = buildCacheKey(inputParams);
        BoolQueryBuilder boolQueryBuilder = Es7Util.setPredicateclause(sideInfo);
        boolQueryBuilder = setInputParams(inputParams, boolQueryBuilder);
        SearchSourceBuilder searchSourceBuilder = initConfiguration();
        searchSourceBuilder.query(boolQueryBuilder);
        searchRequest.source(searchSourceBuilder);

        //async search data
        rhlClient.searchAsync(searchRequest, RequestOptions.DEFAULT, new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {

                List<Object> cacheContent = new ArrayList<>();
                List<BaseRow> rowList = new ArrayList<>();
                SearchHit[] searchHits = searchResponse.getHits().getHits();
                if (searchHits.length > 0) {
                    Elasticsearch7SideTableInfo tableInfo = null;
                    RestHighLevelClient tmpRhlClient = null;
                    try {
                        while (true) {
                            loadDataToCache(searchHits, rowList, cacheContent, input);
                            if (searchHits.length < getFetchSize()) {
                                break;
                            }
                            if (tableInfo == null) {
                                tableInfo = (Elasticsearch7SideTableInfo) sideInfo.getSideTableInfo();
                                tmpRhlClient = Es7Util.getClient(tableInfo.getAddress(), tableInfo.isAuthMesh(), tableInfo.getUserName(), tableInfo.getPassword());
                            }

                            Object[] searchAfterParameter = searchHits[searchHits.length - 1].getSortValues();
                            searchSourceBuilder.searchAfter(searchAfterParameter);
                            searchRequest.source(searchSourceBuilder);
                            searchResponse = tmpRhlClient.search(searchRequest, RequestOptions.DEFAULT);
                            searchHits = searchResponse.getHits().getHits();
                        }
                        dealCacheData(key, CacheObj.buildCacheObj(ECacheContentType.MultiLine, cacheContent));
                        RowDataComplete.completeBaseRow(resultFuture, rowList);
                    } catch (Exception e) {
                        dealFillDataError(input, resultFuture, e);
                    } finally {
                        if (tmpRhlClient != null) {
                            try {
                                tmpRhlClient.close();
                            } catch (IOException ex) {
                                LOG.warn("Failed to shutdown tmpRhlClient.", ex);
                            }
                        }
                    }
                } else {
                    dealMissKey(input, resultFuture);
                    dealCacheData(key, CacheMissVal.getMissKeyObj());
                }
            }

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
        for (Object ele: inputParams.values()) {
            sb.append(ele.toString())
                    .append("_");
        }
        return sb.toString();
    }

    protected List<BaseRow> getRows(BaseRow inputRow, List<Object> cacheContent, List<Object> results) {
        List<BaseRow> rowList = com.google.common.collect.Lists.newArrayList();
        for (Object line : results) {
            BaseRow row = fillData(inputRow, line);
            if (null != cacheContent && openCache()) {
                cacheContent.add(line);
            }
            rowList.add(row);
        }
        return rowList;
    }

    private void loadDataToCache(SearchHit[] searchHits, List<BaseRow> rowList, List<Object> cacheContent, BaseRow copyCrow) {
        List<Object> results = com.google.common.collect.Lists.newArrayList();
        for (SearchHit searchHit : searchHits) {
            Map<String, Object> object = searchHit.getSourceAsMap();
            results.add(object);
        }
        rowList.addAll(getRows(copyCrow, cacheContent, results));
    }

    @Override
    public BaseRow fillData(BaseRow input, Object line) {
        GenericRow genericRow = (GenericRow) input;
        Map<String, Object> cacheInfo = (Map<String, Object>) line;
        GenericRow row = new GenericRow(sideInfo.getOutFieldInfoList().size());
        row.setHeader(input.getHeader());
        String[] fields = sideInfo.getSideTableInfo().getFieldTypes();
        for (Map.Entry<Integer, Integer> entry: sideInfo.getInFieldIndex().entrySet()) {
            Object obj = genericRow.getField(entry.getValue());
            obj = convertTimeIndictorTypeInfo(entry.getValue(), obj);
            row.setField(entry.getKey(), obj);
        }

        for (Map.Entry<Integer, Integer> entry : sideInfo.getSideFieldIndex().entrySet()) {
            if (cacheInfo == null) {
                row.setField(entry.getKey(), null);
            } else {
                Object object = Es7Util.getTarget(cacheInfo.get(sideInfo.getSideFieldNameIndex().get(entry.getKey())), fields[entry.getValue()]);
                row.setField(entry.getKey(), object);
            }
        }

        return row;
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (rhlClient != null) {
            try {
                rhlClient.close();
            } catch (IOException ex) {
                LOG.warn("Failed to shutdown tmpRhlClient.", ex);
            }
        }
    }

    private SearchSourceBuilder initConfiguration() {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(getFetchSize());
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
            boolQueryBuilder = Es7Util.buildFilterCondition(boolQueryBuilder, new PredicateInfo(null, operatorKind, null, fieldName, condition), sideInfo);
        }

        return boolQueryBuilder;
    }

    public int getFetchSize() {
        return 1000;
    }

}
