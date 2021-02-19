package com.dtstack.flink.sql.sink.elasticsearch;

import com.dtstack.flink.sql.sink.elasticsearch.table.ElasticsearchTableInfo;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.table.AbstractTableParser;
import com.dtstack.flink.sql.util.MathUtil;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.util.HashMap;
import java.util.Map;

/**
 * Company: www.dtstack.com
 *
 * @author zhufeng
 * @date 2020-06-18
 */
public class ElasticsearchSinkTest {
    ElasticsearchTableInfo elasticsearchTableInfo = new ElasticsearchTableInfo();
    @Spy
    ElasticsearchSink elasticsearchSink;
    String[] fieldNames;
    TypeInformation<?>[] fieldTypes;


    @Before
    public void setUp() {
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
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void genStreamSinkTest() {
        elasticsearchSink.genStreamSink(elasticsearchTableInfo);
    }

    @Test
    public void configureTest() {
        elasticsearchSink.configure(fieldNames, fieldTypes);
    }

    @Test
    public void getFieldNamesTest() {
        elasticsearchSink.getFieldNames();
    }

    @Test
    public void getFieldTypesTest() {
        elasticsearchSink.getFieldTypes();
    }

    @Test
    public void setBulkFlushMaxActionsTest() {
        elasticsearchSink.setBulkFlushMaxActions(1);
    }

}
