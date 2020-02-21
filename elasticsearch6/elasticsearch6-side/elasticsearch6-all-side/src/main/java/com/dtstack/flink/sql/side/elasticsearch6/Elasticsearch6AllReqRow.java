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
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.dtstack.flink.sql.side.*;
import com.dtstack.flink.sql.side.elasticsearch6.table.Elasticsearch6SideTableInfo;
import com.dtstack.flink.sql.side.elasticsearch6.util.SwitchUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.sql.JoinType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
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
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * @author yinxi
 * @date 2020/1/13 - 1:00
 */
public class Elasticsearch6AllReqRow extends AllReqRow implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch6AllReqRow.class);

    private static final Integer SCROLL_TIME = 1;
    private static final int CONN_RETRY_NUM = 3;
    private static final String KEY_WORD_TYPE = ".keyword";
    private AtomicReference<Map<String, List<Map<String, Object>>>> cacheRef = new AtomicReference<>();
    private String scrollId;
    private Scroll scroll;
    private transient RestHighLevelClient rhlClient;

    public Elasticsearch6AllReqRow(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) {
        super(new Elasticsearch6AllSideInfo(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo));
    }


    @Override
    public void flatMap(CRow value, Collector<CRow> out) throws Exception {
        List<Object> inputParams = Lists.newArrayList();
        for (Integer conValIndex : sideInfo.getEqualValIndex()) {
            Object equalObj = value.row().getField(conValIndex);
            if (equalObj == null) {
                if (sideInfo.getJoinType() == JoinType.LEFT) {
                    Row row = fillData(value.row(), null);
                    out.collect(new CRow(row, value.change()));
                }

                return;
            }

            inputParams.add(equalObj);
        }

        String key = buildKey(inputParams);
        List<Map<String, Object>> cacheList = cacheRef.get().get(key);
        if (CollectionUtils.isEmpty(cacheList)) {
            if (sideInfo.getJoinType() == JoinType.LEFT) {
                Row row = fillData(value.row(), null);
                out.collect(new CRow(row, value.change()));
            } else {
                return;
            }

            return;
        }

        for (Map<String, Object> one : cacheList) {
            out.collect(new CRow(fillData(value.row(), one), value.change()));
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
    protected void initCache() throws SQLException {
        Map<String, List<Map<String, Object>>> newCache = Maps.newConcurrentMap();
        cacheRef.set(newCache);
        try {
            loadData(newCache);
        } catch (Exception e) {
            LOG.error("", e);
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
        }

        cacheRef.set(newCache);
        LOG.info("----- elasticsearch6 all cacheRef reload end:{}", Calendar.getInstance());
    }

    private void loadData(Map<String, List<Map<String, Object>>> tmpCache) throws IOException {
        Elasticsearch6SideTableInfo tableInfo = (Elasticsearch6SideTableInfo) sideInfo.getSideTableInfo();

        try {
            for (int i = 0; i < CONN_RETRY_NUM; i++) {
                try {
                    rhlClient = getClient(tableInfo.getAddress(), tableInfo.isAuthMesh(), tableInfo.getUserName(), tableInfo.getPassword());
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


            // load data from tableA
            SearchSourceBuilder searchSourceBuilder = getSelectFromStatement(sideInfo.getSideTableInfo().getPredicateInfoes());
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

            // get query reults
            searchScroll(searchRequest, tmpCache);


        } catch (Exception e) {
            LOG.error("", e);
        } finally {
            if (!StringUtils.isEmpty(scrollId)) {
                clearScroll();
            }

            if (rhlClient != null) {
                rhlClient.close();
            }
        }

    }

    public void searchScroll(SearchRequest searchRequest, Map<String, List<Map<String, Object>>> tmpCache) throws IOException {
        SearchResponse searchResponse = rhlClient.search(searchRequest, RequestOptions.DEFAULT);
        scrollId = searchResponse.getScrollId();
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        loadToCache(searchHits, tmpCache);

        if (!StringUtils.isEmpty(scrollId)) {
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
            scrollRequest.scroll(scroll);
            while (true) {
                SearchResponse scrollResponse = rhlClient.scroll(scrollRequest, RequestOptions.DEFAULT);
                if (scrollResponse.getHits().getHits() == null || scrollResponse.getHits().getHits().length < 1) {
                    break;
                }
                searchHits = searchResponse.getHits().getHits();
                loadToCache(searchHits, tmpCache);

                scrollId = scrollResponse.getScrollId();
                scrollRequest.scrollId(scrollId);

            }
        }
    }

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

    private void clearScroll() throws IOException {
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        ClearScrollResponse clearScrollResponse = rhlClient.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
        boolean succeeded = clearScrollResponse.isSucceeded();
        LOG.info("Clear scroll response:{}", succeeded);
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

    private SearchSourceBuilder getSelectFromStatement(List<PredicateInfo> predicateInfoes) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        if (predicateInfoes.size() > 0) {
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            for (PredicateInfo info : predicateInfoes) {
                boolQueryBuilder = buildFilterCondition(boolQueryBuilder, info);
            }

            searchSourceBuilder.query(boolQueryBuilder);
        }

        return searchSourceBuilder;
    }

    public BoolQueryBuilder buildFilterCondition(BoolQueryBuilder boolQueryBuilder, PredicateInfo info) {
        switch (info.getOperatorKind()) {
            case "IN":
                return boolQueryBuilder.must(QueryBuilders.termsQuery(textConvertToKeyword(info.getFieldName()), removeSpaces(info.getCondition())));
            case "NOT_IN":
                return boolQueryBuilder.mustNot(QueryBuilders.termsQuery(textConvertToKeyword(info.getFieldName()), removeSpaces(info.getCondition())));
            case "GREATER_THAN_OR_EQUAL":
                return boolQueryBuilder.must(QueryBuilders.rangeQuery(info.getFieldName()).gte(info.getCondition()));
            case "GREATER_THAN":
                return boolQueryBuilder.must(QueryBuilders.rangeQuery(info.getFieldName()).gt(info.getCondition()));
            case "LESS_THAN_OR_EQUAL":
                return boolQueryBuilder.must(QueryBuilders.rangeQuery(info.getFieldName()).lte(info.getCondition()));
            case "LESS_THAN":
                return boolQueryBuilder.must(QueryBuilders.rangeQuery(info.getFieldName()).lt(info.getCondition()));
            case "BETWEEN":
                return boolQueryBuilder.must(QueryBuilders.rangeQuery(info.getFieldName()).gte(StringUtils.split(info.getCondition().toUpperCase(), "AND")[0].trim())
                        .lte(StringUtils.split(info.getCondition().toUpperCase(), "AND")[1].trim()));
            case "IS_NULL":
                return boolQueryBuilder.mustNot(QueryBuilders.existsQuery(info.getFieldName()));
            case "IS_NOT_NULL":
                return boolQueryBuilder.must(QueryBuilders.existsQuery(info.getFieldName()));
            case "EQUALS":
                return boolQueryBuilder.must(QueryBuilders.termQuery(textConvertToKeyword(info.getFieldName()), info.getCondition()));
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
