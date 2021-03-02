package com.dtstack.flink.sql.sink.elasticsearch;

import com.dtstack.flink.sql.sink.elasticsearch.table.ElasticsearchTableInfo;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.table.AbstractTableParser;
import com.dtstack.flink.sql.util.MathUtil;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.transport.TransportClient;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;


import java.net.InetSocketAddress;
import java.util.*;

import static org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase.FlushBackoffType.CONSTANT;
/**
 * Company: www.dtstack.com
 *
 * @author zhufeng
 * @date 2020-06-23
 */
public class ExtendES6ApiCallBridgeTest {
    List<HttpHost> addresses = Collections.singletonList(new HttpHost("172.16.8.193", 9200));
    Map<String, String> clientConfig = new HashMap<>();
    ExtendEs6ApiCallBridge callBridge;
    @Mock
    BulkProcessor.Builder builder;
    @Mock
    BulkItemResponse bulkItemResponse;
    @Mock
    RestHighLevelClient client;
    @Mock
    BulkProcessor.Listener listener;
    ElasticsearchSinkBase.BulkFlushBackoffPolicy flushBackoffPolicy = new ElasticsearchSinkBase.BulkFlushBackoffPolicy();


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
        callBridge = new ExtendEs6ApiCallBridge(addresses, elasticsearchTableInfo);
    }

    //暂时没有ES环境
    @Test
    public void createClientTest() {
        clientConfig.put("cluster.name", "docker-cluster");
        clientConfig.put("xpack.security.user", "elastic:abc123");
        callBridge.createClient(clientConfig);
    }

    @Test
    public void configureBulkProcessorBackoffTest() {
        callBridge.configureBulkProcessorBackoff(builder, flushBackoffPolicy);
        flushBackoffPolicy.setBackoffType(CONSTANT);
        callBridge.configureBulkProcessorBackoff(builder, flushBackoffPolicy);
    }

    @Test
    public void extractFailureCauseFromBulkItemResponseTest() {
        callBridge.extractFailureCauseFromBulkItemResponse(bulkItemResponse);
    }

    @Test
    public void createBulkProcessorBuilderTest() {
        callBridge.createBulkProcessorBuilder(client, listener);
    }
}
