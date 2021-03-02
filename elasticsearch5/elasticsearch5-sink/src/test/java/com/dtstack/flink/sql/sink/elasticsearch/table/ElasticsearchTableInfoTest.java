package com.dtstack.flink.sql.sink.elasticsearch.table;


import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertThat;

/**
 * Company: www.dtstack.com
 *
 * @author zhufeng
 * @date 2020-06-17
 */
public class ElasticsearchTableInfoTest<T> {

    @Spy
    ElasticsearchTableInfo tableInfo;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    //通过反射去遍历ElasticsearchTable中所有的get set 方法
    @Test
    public void getAndSetTest() throws InvocationTargetException, IntrospectionException,
            InstantiationException, IllegalAccessException {
        this.testGetAndSet();
    }

    //check()方法，通过捕捉特定的异常来断言
    @Test
    public void checkTest() {
        tableInfo.setAddress("address");
        tableInfo.setIndex("index");
        tableInfo.setEsType("estype");
        tableInfo.setClusterName("clustername");
        try {
            tableInfo.check();
        } catch (NullPointerException e) {
            assertThat(e.getMessage(), containsString("is required"));
        }

        try {
            tableInfo.setId("d");
            tableInfo.check();
            fail("id must be a numeric type");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("id must be a numeric type"));
        }
        tableInfo.setId("1");
        try {
            tableInfo.setAuthMesh(true);
            tableInfo.check();
        } catch (NullPointerException e) {
            assertThat(e.getMessage(), containsString("is required"));
        }
        tableInfo.setUserName("name");
        tableInfo.setPassword("password");
        assertTrue(tableInfo.check());
    }


    /**
     * model的get和set方法
     * 1.子类返回对应的类型
     * 2.通过反射创建类的实例
     * 3.获取该类所有属性字段，遍历获取每个字段对应的get、set方法，并执行
     */
    private void testGetAndSet() throws IllegalAccessException, InstantiationException, IntrospectionException,
            InvocationTargetException {
        ElasticsearchTableInfo t = new ElasticsearchTableInfo();
        Class<?> modelClass = t.getClass();
        Object obj = modelClass.newInstance();
        Field[] fields = modelClass.getDeclaredFields();
        for (Field f : fields) {
            if (!f.getName().equals("CURR_TYPE")) {
                if (f.getName().equals("aLike")
                        || f.isSynthetic()) {
                    continue;
                }

                PropertyDescriptor pd = new PropertyDescriptor(f.getName(), modelClass);
                Method get = pd.getReadMethod();
                Method set = pd.getWriteMethod();
                set.invoke(obj, get.invoke(obj));
            }
        }
    }


}


