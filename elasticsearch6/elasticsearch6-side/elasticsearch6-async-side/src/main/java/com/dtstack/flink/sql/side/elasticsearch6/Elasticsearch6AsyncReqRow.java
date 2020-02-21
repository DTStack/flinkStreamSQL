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

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.types.Row;

import com.dtstack.flink.sql.enums.ECacheContentType;
import com.dtstack.flink.sql.side.*;
import com.dtstack.flink.sql.side.cache.CacheObj;
import com.dtstack.flink.sql.side.elasticsearch6.table.Elasticsearch6SideTableInfo;
import com.dtstack.flink.sql.side.elasticsearch6.util.SwitchUtil;
import com.dtstack.flink.sql.util.ParseUtils;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author yinxi
 * @date 2020/2/13 - 13:10
 */
public class Elasticsearch6AsyncReqRow extends AsyncReqRow implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch6AsyncReqRow.class);
    private static final Integer SCROLL_TIME = 1 ;
    private static final String KEY_WORD_TYPE = ".keyword";
    private transient RestHighLevelClient rhlClient;
    private transient Scroll scroll;
    private List<String> sqlJoinCompareOperate= Lists.newArrayList();

    public Elasticsearch6AsyncReqRow(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) {
        super(new Elasticsearch6AsyncSideInfo(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo));
        SqlNode conditionNode = joinInfo.getCondition();
        ParseUtils.parseJoinCompareOperate(conditionNode, sqlJoinCompareOperate);
    }


    @Override
    public void asyncInvoke(CRow input, ResultFuture<CRow> resultFuture) throws Exception {
        CRow copyCrow = new CRow(input.row(), input.change());
        List<Object> inputParams = Lists.newArrayList();
        for (Integer conValIndex : sideInfo.getEqualValIndex()) {
            Object equalObj = copyCrow.row().getField(conValIndex);
            if (equalObj == null) {
                dealMissKey(copyCrow, resultFuture);
                return;
            }
            inputParams.add(equalObj);
        }

        String key = buildCacheKey(inputParams);
        if (openCache()) {
            CacheObj val = getFromCache(key);
            if (val != null) {
                if (ECacheContentType.MissVal == val.getType()) {
                    dealMissKey(copyCrow, resultFuture);
                    return;
                } else if (ECacheContentType.MultiLine == val.getType()) {
                    try {
                        List<CRow> rowList = getRows(copyCrow, null, (List) val.getContent());
                        resultFuture.complete(rowList);
                    } catch (Exception e) {
                        dealFillDataError(resultFuture, e, copyCrow);
                    }
                } else {
                    resultFuture.completeExceptionally(new RuntimeException("not support cache obj type " + val.getType()));
                }
                return;
            }
        }

        Elasticsearch6SideTableInfo tableInfo = (Elasticsearch6SideTableInfo) sideInfo.getSideTableInfo();

        rhlClient = getClient(tableInfo.getAddress(), tableInfo.isAuthMesh(), tableInfo.getUserName(), tableInfo.getPassword());

        // load data from tableA
        SearchSourceBuilder searchSourceBuilder = getSelectFromStatement(inputParams, sideInfo.getSideTableInfo().getPredicateInfoes());

        searchSourceBuilder.size(getFetchSize());
        SearchRequest searchRequest = new SearchRequest();
        scroll = new Scroll(TimeValue.timeValueMinutes(SCROLL_TIME));
        searchRequest.scroll(scroll);

        // determine existence of index
        String index = tableInfo.getIndex().trim();
        if (!StringUtils.isEmpty(index)) {
            // strip leading and trailing spaces from a string
            String[] indexes = StringUtils.split(index, ",");
            for (int i = 0; i < indexes.length; i++) {
                indexes[i] = indexes[i].trim();
            }

            searchRequest.indices(indexes);

        }

        // determine existence of type
        String type = tableInfo.getEsType().trim();
        if (!StringUtils.isEmpty(type)) {
            // strip leading and trailing spaces from a string
            String[] types = StringUtils.split(type, ",");
            for (int i = 0; i < types.length; i++) {
                types[i] = types[i].trim();
            }

            searchRequest.types(types);
        }

        // add query condition
        searchRequest.source(searchSourceBuilder);

        // 异步查询数据

        rhlClient.searchAsync(searchRequest, RequestOptions.DEFAULT, new ActionListener<SearchResponse>() {

            //成功响应时
            @Override
            public void onResponse(SearchResponse searchResponse) {
                String scrollId = searchResponse.getScrollId();

                List<Object> cacheContent = Lists.newArrayList();
                List<Object> results = Lists.newArrayList();
                loadDataToCache(searchResponse, key, results, cacheContent, copyCrow, resultFuture);

                if (!StringUtils.isEmpty(scrollId)) {
                    SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                    scrollRequest.scroll(scroll);
                    while (true) {
                        SearchResponse scrollResponse = null;
                        try {
                            scrollResponse = rhlClient.scroll(scrollRequest, RequestOptions.DEFAULT);
                        } catch (IOException e) {
                            resultFuture.completeExceptionally(new RuntimeException("Request failed!"));
                        }

                        if (scrollResponse.getHits().getHits() == null || scrollResponse.getHits().getHits().length < 1) {
                            break;
                        }
                        loadDataToCache(searchResponse, key, results, cacheContent, copyCrow, resultFuture);

                        scrollId = scrollResponse.getScrollId();
                        scrollRequest.scrollId(scrollId);

                    }
                }
                try {
                    clearScroll(scrollId);
                } catch (IOException e) {
                    LOG.warn("Clear scroll response:{}: failure", e);
                }

            }

            private void loadDataToCache(SearchResponse searchResponse, String key, List<Object> results, List<Object> cacheContent, CRow copyCrow, ResultFuture<CRow> resultFuture) {
                SearchHit[] searchHits = searchResponse.getHits().getHits();
                if (searchHits.length > 0) {
                    try {
                        for (SearchHit searchHit : searchHits) {
                            Map<String, Object> object = searchHit.getSourceAsMap();
                            results.add(object);
                        }

                        List<CRow> rowList = getRows(copyCrow, cacheContent, results);
                        dealCacheData(key, CacheObj.buildCacheObj(ECacheContentType.MultiLine, cacheContent));
                        resultFuture.complete(rowList);
                    } catch (Exception e) {
                        dealFillDataError(resultFuture, e, copyCrow);
                    }
                } else {
                    dealMissKey(copyCrow, resultFuture);
                    dealCacheData(key, CacheMissVal.getMissKeyObj());
                }
            }

            private void clearScroll(String scrollId) throws IOException {
                ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
                clearScrollRequest.addScrollId(scrollId);
                ClearScrollResponse clearScrollResponse = rhlClient.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
                boolean succeeded = clearScrollResponse.isSucceeded();
                LOG.info("Clear scroll response:{}", succeeded);
            }

            // 响应失败处理
            @Override
            public void onFailure(Exception e) {
                LOG.error("" + e);
                resultFuture.completeExceptionally(new RuntimeException("Response failed!"));
            }
        });

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
                Object object = SwitchUtil.getTarget(cacheInfo.get(entry.getValue()), fields[entry.getValue()]);
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

    public String buildCacheKey(List equalValList) {
        StringBuilder sb = new StringBuilder();
        for (Object ele : equalValList) {
            sb.append(ele.toString())
                    .append("_");
        }

        return sb.toString();
    }


    public RestHighLevelClient getClient(String esAddress, Boolean isAuthMesh, String userName, String password) {

        List<HttpHost> httpHostList = new ArrayList<>();
        String[] address = StringUtils.split(esAddress, ",");
        for (String addr : address) {
            String[] infoArray = StringUtils.split(addr, ":");
            int port = 9200;
            String host = infoArray[0].trim();
            if (infoArray.length > 1) {
                port = Integer.valueOf(infoArray[1].trim());
            }
            httpHostList.add(new HttpHost(host, port, "http"));
        }

        RestClientBuilder restClientBuilder = RestClient.builder(httpHostList.toArray(new HttpHost[httpHostList.size()]));

        if (isAuthMesh) {
            // 进行用户和密码认证
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName.trim(), password.trim()));
            restClientBuilder.setHttpClientConfigCallback(httpAsyncClientBuilder ->
                    httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        }

        RestHighLevelClient rhlClient = new RestHighLevelClient(restClientBuilder);

        if (LOG.isInfoEnabled()) {
            LOG.info("Pinging Elasticsearch cluster via hosts {} ...", httpHostList);
        }

        try {
            if (!rhlClient.ping()) {
                throw new RuntimeException("There are no reachable Elasticsearch nodes!");
            }
        } catch (IOException e) {
            LOG.warn("", e);
        }


        if (LOG.isInfoEnabled()) {
            LOG.info("Created Elasticsearch RestHighLevelClient connected to {}", httpHostList.toString());
        }

        return rhlClient;

    }

    public int getFetchSize() {
        return 1000;
    }

    private SearchSourceBuilder getSelectFromStatement(List<Object> inputParams, List<PredicateInfo> predicateInfoes) {

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();


        for (int i = 0; i < sqlJoinCompareOperate.size(); i++) {
            String fieldName = sideInfo.getEqualFieldList().get(i);
            String operatorKind = sqlJoinCompareOperate.get(sideInfo.getEqualFieldList().indexOf(fieldName));
            String condition = String.valueOf(inputParams.get(i));
            boolQueryBuilder = buildFilterCondition(boolQueryBuilder, new PredicateInfo(null, operatorKind, null, fieldName, condition));
        }

        if (predicateInfoes.size() > 0) {
            for (PredicateInfo info : predicateInfoes) {
                boolQueryBuilder = buildFilterCondition(boolQueryBuilder, info);
            }
        }
        searchSourceBuilder.query(boolQueryBuilder);
        return searchSourceBuilder;
    }

    public BoolQueryBuilder buildFilterCondition(BoolQueryBuilder boolQueryBuilder, PredicateInfo info) {
        switch (info.getOperatorKind()) {
            case "IN":
                return boolQueryBuilder.must(QueryBuilders.termsQuery(textConvertToKeyword(info.getFieldName()), removeSpaces(info.getCondition())));
            case "NOT_IN":
                return boolQueryBuilder.mustNot(QueryBuilders.termsQuery(textConvertToKeyword(info.getFieldName()), removeSpaces(info.getCondition())));
            case ">=":
            case "GREATER_THAN_OR_EQUAL":
                return boolQueryBuilder.must(QueryBuilders.rangeQuery(info.getFieldName()).gte(info.getCondition()));
            case ">":
            case "GREATER_THAN":
                return boolQueryBuilder.must(QueryBuilders.rangeQuery(info.getFieldName()).gt(info.getCondition()));
            case "<=":
            case "LESS_THAN_OR_EQUAL":
                return boolQueryBuilder.must(QueryBuilders.rangeQuery(info.getFieldName()).lte(info.getCondition()));
            case "<":
            case "LESS_THAN":
                return boolQueryBuilder.must(QueryBuilders.rangeQuery(info.getFieldName()).lt(info.getCondition()));
            case "BETWEEN":
                return boolQueryBuilder.must(QueryBuilders.rangeQuery(info.getFieldName()).gte(StringUtils.split(info.getCondition().toUpperCase(), "AND")[0].trim())
                        .lte(StringUtils.split(info.getCondition().toUpperCase(), "AND")[1].trim()));
            case "IS_NULL":
                return boolQueryBuilder.mustNot(QueryBuilders.existsQuery(info.getFieldName()));
            case "IS_NOT_NULL":
                return boolQueryBuilder.must(QueryBuilders.existsQuery(info.getFieldName()));
            case "=":
            case "EQUALS":
                return boolQueryBuilder.must(QueryBuilders.termQuery(textConvertToKeyword(info.getFieldName()), info.getCondition()));
            case "<>":
            case "NOT_EQUALS":
                return boolQueryBuilder.mustNot(QueryBuilders.termQuery(textConvertToKeyword(info.getFieldName()), info.getCondition()));
            default:
                try {
                    throw new Exception("elasticsearch6 does not support this operation: " + info.getOperatorName());
                } catch (Exception e) {

                    e.printStackTrace();
                    LOG.error(e.getMessage());
                }
                return boolQueryBuilder;
        }

    }

    public String[] removeSpaces(String str) {
        String[] split = StringUtils.split(str, ",");
        String[] result = new String[split.length];
        Arrays.asList(split).stream().map(f -> f.trim()).collect(Collectors.toList()).toArray(result);
        return result;
    }

    public String textConvertToKeyword(String fieldName) {
        String[] sideFieldNames = StringUtils.split(sideInfo.getSideSelectFields().trim(), ",");
        String[] sideFieldTypes = sideInfo.getSideTableInfo().getFieldTypes();
        int fieldIndex = sideInfo.getSideTableInfo().getFieldList().indexOf(fieldName.trim());
        String fieldType = sideFieldTypes[fieldIndex];
        switch (fieldType.toLowerCase()) {
            case "varchar":
            case "char":
            case "text":
                return fieldName + KEY_WORD_TYPE;
            default:
                return fieldName;
        }
    }
}

