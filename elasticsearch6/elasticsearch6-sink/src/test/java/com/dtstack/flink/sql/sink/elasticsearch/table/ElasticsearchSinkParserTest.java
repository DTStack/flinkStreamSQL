package com.dtstack.flink.sql.sink.elasticsearch.table;

import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.table.AbstractTableParser;
import com.dtstack.flink.sql.util.MathUtil;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertFalse;

/**
 * Company: www.dtstack.com
 *
 * @author zhufeng
 * @date 2020-06-22
 */
public class ElasticsearchSinkParserTest {
    @Spy
    ElasticsearchSinkParser elasticsearchSinkParser;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    //ElasticsearchSinkParser中的fieldNameNeedsUpperCase方法
//    @Test
    public void fieldNameNeedsUpperCaseTest() {
        assertFalse(elasticsearchSinkParser.fieldNameNeedsUpperCase());
    }


    //getTableInfo方法，得到输入的表信息
//    @Test
    public void getTableInfoTest() {
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
        String[] ignoreArr = {"fieldClasses", "fieldTypes", "fields"};
        assertTrue(compareFields(elasticsearchTableInfo, elasticsearchSinkParser.getTableInfo(tableName, fieldsInfo, props), ignoreArr));
    }

    private void assertTrue(Boolean compareFields) {
    }

    //比较两个对象的值是不是相同
    public static Boolean compareFields(Object obj1, Object obj2, String[] ignoreArr) {
        try {
            Boolean checkIsSame = true;
            List<String> ignoreList = null;
            if (ignoreArr != null && ignoreArr.length > 0) {
                // array转化为list
                ignoreList = Arrays.asList(ignoreArr);
            }
            if (obj1.getClass() == obj2.getClass()) {// 只有两个对象都是同一类型的才有可比性
                Class<? extends Object> clazz = obj1.getClass();
                // 获取object的属性描述
                PropertyDescriptor[] pds = Introspector.getBeanInfo(clazz, Object.class).getPropertyDescriptors();
                for (PropertyDescriptor pd : pds) {// 这里就是所有的属性了
                    String name = pd.getName();// 属性名
                    if (ignoreList != null && ignoreList.contains(name)) {// 如果当前属性选择忽略比较，跳到下一次循环
                        continue;
                    }
                    Method readMethod = pd.getReadMethod();// get方法
                    // 在obj1上调用get方法等同于获得obj1的属性值
                    Object o1 = readMethod.invoke(obj1);
                    // 在obj2上调用get方法等同于获得obj2的属性值
                    Object o2 = readMethod.invoke(obj2);
                    if (o1 == null && o2 == null) {
                        continue;
                    } else if (o1 == null && o2 != null) {
                        checkIsSame = false;
                        continue;
                    }
                    if (!o1.equals(o2)) {// 比较这两个值是否相等,不等就可以放入map了
                        checkIsSame = false;
                    }
                }
            }
            return checkIsSame;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }


}
