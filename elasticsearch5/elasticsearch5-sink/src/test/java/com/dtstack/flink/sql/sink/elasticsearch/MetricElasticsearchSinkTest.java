package com.dtstack.flink.sql.sink.elasticsearch;

import com.dtstack.flink.sql.sink.elasticsearch.table.ElasticsearchTableInfo;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.table.AbstractTableParser;
import com.dtstack.flink.sql.util.MathUtil;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.http.HttpHost;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;

/**
 * Company: www.dtstack.com
 *
 * @author zhufeng
 * @date 2020-06-19
 */
public class MetricElasticsearchSinkTest {
    List<HttpHost> addresses = Collections.singletonList(new HttpHost("172.16.8.193", 9200));
    ElasticsearchSinkBase.BulkFlushBackoffPolicy flushBackoffPolicy = new ElasticsearchSinkBase.BulkFlushBackoffPolicy();
    Map userConfig = new HashMap();
    MetricElasticsearchSink sink;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        String tableName = "MyResult";
        String fieldsInfo = "pv varchar,  channel varchar";
        Map<String, Object> props = new HashMap<>();
        props.put("cluster", "docker-cluster");
        props.put("password", "abc123");
        props.put("address", "172.16.8.193:9200");
        props.put("parallelism", "1");
        props.put("index", "myresult");
        props.put("updatemode", "append");
        props.put("id", "1");
        props.put("type", "elasticsearch");
        props.put("estype", "elasticsearch");
        props.put("authmesh", "true");
        props.put("username", "elastic");
        AbstractTableParser tableParser = new AbstractTableParser() {
            @Override
            public AbstractTableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) throws Exception {
                return null;
            }
        };
        ElasticsearchTableInfo elasticsearchTableInfo = new ElasticsearchTableInfo();
        elasticsearchTableInfo.setName(tableName);
        tableParser.parseFieldsInfo(fieldsInfo, elasticsearchTableInfo);
        elasticsearchTableInfo.setAddress((String) props.get("address"));
        elasticsearchTableInfo.setClusterName((String) props.get("cluster"));
        elasticsearchTableInfo.setId((String) props.get("id"));
        elasticsearchTableInfo.setIndex((String) props.get("index"));
        elasticsearchTableInfo.setEsType((String) props.get("estype"));
        String authMeshStr = (String) props.get("authmesh");
        if (authMeshStr != null & "true".equals(authMeshStr)) {
            elasticsearchTableInfo.setAuthMesh(MathUtil.getBoolean(authMeshStr));
            elasticsearchTableInfo.setUserName(MathUtil.getString(props.get("username")));
            elasticsearchTableInfo.setPassword(MathUtil.getString(props.get("password")));
        }
        ElasticsearchSinkFunction elasticsearchSinkFunction = mock(ElasticsearchSinkFunction.class);
        try {
            sink = new MetricElasticsearchSink(userConfig, addresses, elasticsearchSinkFunction, elasticsearchTableInfo);
        } catch (Exception ignored) {
        }

    }

    @Test
    public void Test() {

    }
}


