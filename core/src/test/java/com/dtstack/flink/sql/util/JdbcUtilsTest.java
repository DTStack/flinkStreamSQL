package com.dtstack.flink.sql.util;

import org.junit.Test;

public class JdbcUtilsTest {
    @Test
    public void testForName(){
        JDBCUtils.forName("java.lang.String");
    }

    @Test
    public void testForNameWithClassLoader(){
        JDBCUtils.forName("java.lang.String", Thread.currentThread().getContextClassLoader());
    }
}
