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

import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.types.Row;

import com.dtstack.flink.sql.enums.ECacheContentType;
import com.dtstack.flink.sql.side.AsyncReqRow;
import com.dtstack.flink.sql.side.CacheMissVal;
import com.dtstack.flink.sql.side.SideInfo;
import com.dtstack.flink.sql.side.cache.CacheObj;
import com.dtstack.flink.sql.side.elasticsearch6.table.Elasticsearch6SideTableInfo;
import com.dtstack.flink.sql.side.elasticsearch6.util.SwitchUtil;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author yinxi
 * @date 2020/2/13 - 13:10
 */
public class Elasticsearch6AsyncReqRow extends AsyncReqRow {

    private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch6AsyncReqRow.class);

    private static final int CONN_RETRY_NUM = 3;

    public final static int DEFAULT_VERTX_EVENT_LOOP_POOL_SIZE = 1;

    public final static int DEFAULT_VERTX_WORKER_POOL_SIZE = Runtime.getRuntime().availableProcessors() * 2;

    public final static int DEFAULT_MAX_DB_CONN_POOL_SIZE = DEFAULT_VERTX_EVENT_LOOP_POOL_SIZE + DEFAULT_VERTX_WORKER_POOL_SIZE;

    public final static int DEFAULT_IDLE_CONNECTION_TEST_PEROID = 60;

    public final static boolean DEFAULT_TEST_CONNECTION_ON_CHECKIN = true;

    public final static String PREFERRED_TEST_QUERY_SQL = "select 1 from dual";

    private transient RestHighLevelClient rhlClient;

    public Elasticsearch6AsyncReqRow(SideInfo sideInfo) {
        super(sideInfo);
    }

    @Override
    public void asyncInvoke(Row input, ResultFuture<Row> resultFuture) throws Exception {
        Row inputRow = Row.copy(input);
        List<Object> inputParams = Lists.newArrayList();
//        JsonArray inputParams = new JsonArray();
        for (Integer conValIndex : sideInfo.getEqualValIndex()) {
            Object equalObj = inputRow.getField(conValIndex);
            if (equalObj == null) {
                dealMissKey(inputRow, resultFuture);
                return;
            }
            inputParams.add(equalObj);
        }

        String key = buildCacheKey(inputParams);
        if (openCache()) {
            CacheObj val = getFromCache(key);
            if (val != null) {
                if (ECacheContentType.MissVal == val.getType()) {
                    dealMissKey(inputRow, resultFuture);
                    return;
                } else if (ECacheContentType.MultiLine == val.getType()) {
                    try {
                        List<Row> rowList = getRows(inputRow, null, (List) val.getContent());
                        resultFuture.complete(rowList);
                    } catch (Exception e) {
                        dealFillDataError(resultFuture, e, inputRow);
                    }
                } else {
                    resultFuture.completeExceptionally(new RuntimeException("not support cache obj type " + val.getType()));
                }
                return;
            }
        }

        Elasticsearch6SideTableInfo tableInfo = (Elasticsearch6SideTableInfo) sideInfo.getSideTableInfo();
        RestHighLevelClient rhlClient = null;
        try{
            rhlClient = getClient(tableInfo.getAddress(), tableInfo.isAuthMesh(), tableInfo.getUserName(), tableInfo.getPassword(), tableInfo.getTimeout());

            // load data from tableA
            SearchSourceBuilder searchSourceBuilder = tableInfo.getSearchSourceBuilder();
//            searchSourceBuilder.size(getFetchSize());
            SearchRequest searchRequest = new SearchRequest();

            // determine existence of index
            String index = tableInfo.getIndex().trim();
            if(!StringUtils.isEmpty(index)){
                // strip leading and trailing spaces from a string
                String[] indexes = StringUtils.split(index, ",");
                for(int i=0; i < indexes.length; i++ ){
                    indexes[i] = indexes[i].trim();
                }

                searchRequest.indices(indexes);

            }

            // determine existence of type
            String type = tableInfo.getEsType().trim();
            if(!StringUtils.isEmpty(type)){
                // strip leading and trailing spaces from a string
                String[] types = StringUtils.split(type, ",");
                for(int i=0; i < types.length; i++ ){
                    types[i] = types[i].trim();
                }

                searchRequest.types(types);
            }

            // add query condition
            searchRequest.source(searchSourceBuilder);

            // 异步查询数据

            rhlClient.searchAsync(searchRequest, RequestOptions.DEFAULT ,new ActionListener<SearchResponse>() {

                //成功响应时
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    SearchHit[] searchHits = searchResponse.getHits().getHits();
                    List<Object> cacheContent = Lists.newArrayList();
                    List<Object> results = Lists.newArrayList();
                    if(searchHits.length > 0){
                        try{
                            for(SearchHit searchHit : searchHits){
                                Map<String, Object> object = searchHit.getSourceAsMap();
                                results.add(object);
                            }

                            List<Row> rowList = getRows(inputRow, cacheContent, results);
                            dealCacheData(key,CacheObj.buildCacheObj(ECacheContentType.MultiLine, cacheContent));
                            resultFuture.complete(rowList);
                        }catch (Exception e){
                            dealFillDataError(resultFuture, e, inputRow);
                        }
                    }else{
                        dealMissKey(inputRow, resultFuture);
                        dealCacheData(key, CacheMissVal.getMissKeyObj());
                    }


                }

                // 响应失败处理
                @Override
                public void onFailure(Exception e) {
                    resultFuture.completeExceptionally(new RuntimeException("Response timeout!"));
                }
            });




        } catch (Exception e) {
            LOG.error("", e);
        } finally {
            if (rhlClient != null) {
                rhlClient.close();
            }
        }

    }

    protected List<Row> getRows(Row inputRow, List<Object> cacheContent, List<Object> results) {
        List<Row> rowList = Lists.newArrayList();
        for (Object line : results) {
            Row row = fillData(inputRow, line);
            if (null != cacheContent && openCache()) {
                cacheContent.add(line);
            }
            rowList.add(row);
        }
        return rowList;
    }

    @Override
    public Row fillData(Row input, Object line) {
//        JsonArray jsonArray = (JsonArray) line;
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

//    public void setRdbSQLClient(SQLClient rdbSQLClient) {
//        this.rdbSQLClient = rdbSQLClient;
//    }

    public RestHighLevelClient getClient(String esAddress, Boolean isAuthMesh, String userName, String password, Integer timeout) {

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

        if (timeout != null) {
            restClientBuilder.setMaxRetryTimeoutMillis(timeout * 1000);
        }

        if (isAuthMesh) {
            // 进行用户和密码认证
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));
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

}

