package com.dtstack.flink.sql.side.elasticsearch6.util;

import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.BaseSideInfo;
import com.dtstack.flink.sql.side.PredicateInfo;
import com.dtstack.flink.sql.side.elasticsearch6.table.Elasticsearch6SideTableInfo;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.table.AbstractTableParser;
import com.dtstack.flink.sql.util.MathUtil;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;


import static org.mockito.Mockito.mock;

/**
 * Company: www.dtstack.com
 *
 * @author zhufeng
 * @date 2020-07-02
 */
public class Es6UtilTest {

    @Test
    public void getClientTest() {
        String esAddress = "172.16.8.193:9200";
        boolean isAuthMesh = true;
        String userName = "elastic";
        String password = "abc123";
        Es6Util.getClient(esAddress, isAuthMesh, userName, password);
        BaseSideInfo sideInfo = mock(BaseSideInfo.class);
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
            elasticsearchTableInfo.setAuthMesh(com.dtstack.flink.sql.util.MathUtil.getBoolean(authMeshStr));
            elasticsearchTableInfo.setUserName(com.dtstack.flink.sql.util.MathUtil.getString(props.get("username")));
            elasticsearchTableInfo.setPassword(MathUtil.getString(props.get("password")));
        }
        AbstractSideTableInfo tableInfo = mock(AbstractSideTableInfo.class);

        sideInfo.setSideTableInfo(tableInfo);
        try {
            Es6Util.setSearchRequest(sideInfo);
            Es6Util.setPredicateclause(sideInfo);
        } catch (Exception ignored) {
        }
    }

    @Test
    public void buildFilterConditionTest() {
        BoolQueryBuilder boolQueryBuilder = mock(BoolQueryBuilder.class);
        BaseSideInfo sideInfo = mock(BaseSideInfo.class);
        PredicateInfo info = new PredicateInfo("zf", "IN", "mytable", "name", "varchar");
        try {
            Es6Util.buildFilterCondition(boolQueryBuilder, info, sideInfo);
        } catch (Exception ignored) {
        }

    }

    @Test
    public void removeSpaceAndApostropheTest() {
        Es6Util.removeSpaceAndApostrophe("zftest");
    }

    @Test
    public void textConvertToKeywordTest() {
        BaseSideInfo sideInfo = mock(BaseSideInfo.class);
        try {
            Es6Util.textConvertToKeyword("name", sideInfo);
        } catch (Exception ignored) {
        }
    }

    @Test
    public void es6UtilTest() {
        Integer test1 = 1;
        String targetType = "smallint";
        Es6Util.getTarget(test1, targetType);
        String targetType2 = "long";
        Es6Util.getTarget(test1, targetType2);
        Boolean test2 = true;
        String targetType3 = "boolean";
        Es6Util.getTarget(test2, targetType3);
        Byte test3 = 1;
        String targetType4 = "blob";
        Es6Util.getTarget(test3, targetType4);
        String test4 = "name";
        String targetType5 = "varchar";
        Es6Util.getTarget(test4, targetType5);
        Float test5 = 1.0f;
        String targetType6 = "real";
        Es6Util.getTarget(test5, targetType6);
        BigDecimal test7 = new BigDecimal("1234567890");
        String targetType8 = "decimal";
        Es6Util.getTarget(test7, targetType8);
        java.sql.Date date = new java.sql.Date(System.currentTimeMillis());
        String targetType9 = "date";
        Es6Util.getTarget(date, targetType9);
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        String targetType10 = "timestamp";
        Es6Util.getTarget(timestamp, targetType10);
    }
}
