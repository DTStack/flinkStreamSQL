package com.dtstack.flink.sql.util;

import org.junit.Test;

public class ReflectionUtilTest extends Object{

    @Test
    public void getDeclaredMethod(){
        ReflectionUtilTest reflectionUtilTest = new ReflectionUtilTest();
        ReflectionUtils.getDeclaredMethod(reflectionUtilTest, "getDeclaredField", String.class);
    }

    @Test
    public void getDeclaredField(){
        ReflectionUtilTest reflectionUtilTest = new ReflectionUtilTest();
        ReflectionUtils.getDeclaredField(reflectionUtilTest, "id");
    }

}
