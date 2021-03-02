

package com.dtstack.flink.sql.side.elasticsearch6.table;


import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.table.AbstractTableParser;
import com.dtstack.flink.sql.util.MathUtil;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;

/**
 * Company: www.dtstack.com
 *
 * @author zhufeng
 * @date 2020-07-02
 */
public class Elasticsearch6SideParserTest {

    @Spy
    Elasticsearch6SideParser elasticsearch6SideParser;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    //ElasticsearchSinkParser中的fieldNameNeedsUpperCase方法
    @Test
    public void fieldNameNeedsUpperCaseTest() {
        assertFalse(elasticsearch6SideParser.fieldNameNeedsUpperCase());
    }


    //getTableInfo方法，得到输入的表信息
    @Test
    public void getTableInfoTest() {
        String tableName = "MyResult";
        String fieldsInfo = "pv varchar,  channel varchar, PRIMARY  KEY  (pv)";
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
        Elasticsearch6SideTableInfo elasticsearchTableInfo = new Elasticsearch6SideTableInfo();
        elasticsearchTableInfo.setName(tableName);
        tableParser.parseFieldsInfo(fieldsInfo, elasticsearchTableInfo);
        elasticsearchTableInfo.setAddress((String) props.get("address"));
        elasticsearchTableInfo.setClusterName((String) props.get("cluster"));
        elasticsearchTableInfo.setIndex((String) props.get("index"));
        elasticsearchTableInfo.setEsType((String) props.get("estype"));
        String authMeshStr = (String) props.get("authmesh");
        if (authMeshStr != null & "true".equals(authMeshStr)) {
            elasticsearchTableInfo.setAuthMesh(MathUtil.getBoolean(authMeshStr));
            elasticsearchTableInfo.setUserName(MathUtil.getString(props.get("username")));
            elasticsearchTableInfo.setPassword(MathUtil.getString(props.get("password")));
        }
        elasticsearch6SideParser.getTableInfo(tableName, fieldsInfo, props);
    }

}
