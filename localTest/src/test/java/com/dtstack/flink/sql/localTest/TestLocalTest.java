package com.dtstack.flink.sql.localTest;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * @author tiezhu
 * @Date 2020/7/8 Wed
 * Company dtstack
 */
public class TestLocalTest {

    @Test
    public void testReadSQL() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String result = "this is a test";
        String sqlPath = "/Users/wtz4680/Desktop/ideaProject/flinkStreamSQL/localTest/src/test/resources/test.txt";
        Class<LocalTest> testClass = LocalTest.class;
        Method method = testClass.getDeclaredMethod("readSQL", String.class);
        method.setAccessible(true);
        Assert.assertEquals(result, method.invoke(new LocalTest(), sqlPath));
    }
}
