package com.dtstack.flink.sql.side.elasticsearch6;

import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.dtstack.flink.sql.side.AllReqRow;
import com.dtstack.flink.sql.side.SideInfo;
import com.dtstack.flink.sql.side.elasticsearch6.table.Elasticsearch6SideTableInfo;
import com.dtstack.flink.sql.side.elasticsearch6.util.SwitchUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.sql.JoinType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author yinxi
 * @date 2020/1/13 - 1:00
 */
public class Elasticsearh6AllReqRow extends AllReqRow {

    private static final Logger LOG = LoggerFactory.getLogger(Elasticsearh6AllReqRow.class);
    private static final int CONN_RETRY_NUM = 3;
    private AtomicReference<Map<String, List<Map<String, Object>>>> cacheRef = new AtomicReference<>();

    public Elasticsearh6AllReqRow(SideInfo sideInfo) {
        super(sideInfo);
    }

    @Override
    public void flatMap(Row value, Collector<Row> out) throws Exception {
        List<Object> inputParams = Lists.newArrayList();
        for (Integer conValIndex : sideInfo.getEqualValIndex()) {
            Object equalObj = value.getField(conValIndex);
            if (equalObj == null) {
                if (sideInfo.getJoinType() == JoinType.LEFT) {
                    Row row = fillData(value, null);
                    out.collect(row);
                }

                return;
            }

            inputParams.add(equalObj);
        }

        String key = buildKey(inputParams);
        List<Map<String, Object>> cacheList = cacheRef.get().get(key);
        if(CollectionUtils.isEmpty(cacheList)){
            if(sideInfo.getJoinType() == JoinType.LEFT){
                Row row = fillData(value, null);
                out.collect(row);
            }else{
                return;
            }

            return;
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
        RestHighLevelClient rhlClient = null;

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
                        String connInfo = "address: " + tableInfo.getAddress() + ";userName:" + tableInfo.getUserName() + ",pwd:" + tableInfo.getPassword();
                        LOG.warn("get conn fail, wait for 5 sec and try again, connInfo:" + connInfo);
                        Thread.sleep(5 * 1000);
                    } catch (InterruptedException e1) {
                        LOG.error("", e1);
                    }
                }
            }

            // load data from tableA
            SearchSourceBuilder searchSourceBuilder = Elasticsearch6AllSideInfo.searchSourceBuilder;
            searchSourceBuilder.size(getFetchSize());
            SearchRequest searchRequest = new SearchRequest(tableInfo.getIndex());
            searchRequest.types(tableInfo.getEsType());
            searchRequest.source(searchSourceBuilder);

            SearchResponse searchResponse = rhlClient.search(searchRequest);
            SearchHit[] searchHits = searchResponse.getHits().getHits();
            String[] sideFieldNames = sideInfo.getSideSelectFields().split(",");
            String[] fields = sideInfo.getSideTableInfo().getFieldTypes();

            Map<String, Object> oneRow = Maps.newHashMap();
            for (SearchHit searchHit : searchHits) {
                for(String fieldName : sideFieldNames){
                    Object object = searchHit.getSourceAsMap().get(fieldName.trim());
                    int fieldIndex = sideInfo.getSideTableInfo().getFieldList().indexOf(fieldName.trim());
                    object = SwitchUtil.getTarget(object, fields[fieldIndex]);
                    oneRow.put(fieldName.trim(), object);
                }

            }

            String cacheKey = buildKey(oneRow, sideInfo.getEqualFieldList());
            List<Map<String, Object>> list = tmpCache.computeIfAbsent(cacheKey, key -> Lists.newArrayList());
            list.add(oneRow);

        } catch (Exception e) {
            LOG.error("", e);
        } finally {
            if (rhlClient != null) {
                rhlClient.close();
            }
        }

    }

    public RestHighLevelClient getClient(String esAddress, Boolean isAuthMesh, String userName, String password) {
        List<HttpHost> httpHostList = new ArrayList<>();
        String[] address = esAddress.split(",");
        for (String addr : address) {
            String[] infoArray = addr.split(":");
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
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName, password));
            restClientBuilder.setHttpClientConfigCallback(httpAsyncClientBuilder ->
                    httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        }

        RestHighLevelClient rhlClient = new RestHighLevelClient(restClientBuilder);

        return rhlClient;

    }

    public int getFetchSize() {
        return 1000;
    }
}
