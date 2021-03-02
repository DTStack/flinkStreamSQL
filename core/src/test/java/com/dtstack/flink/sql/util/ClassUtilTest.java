package com.dtstack.flink.sql.util;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

public class ClassUtilTest {

    @Test
    public void testStringConvertClass(){
        Assert.assertEquals(ClassUtil.stringConvertClass("bit"), Boolean.class);
        Assert.assertEquals(ClassUtil.stringConvertClass("int"), Integer.class);
        Assert.assertEquals(ClassUtil.stringConvertClass("blob"), Byte.class);
        Assert.assertEquals(ClassUtil.stringConvertClass("bigintunsigned"), Long.class);
        Assert.assertEquals(ClassUtil.stringConvertClass("text"), String.class);
        Assert.assertEquals(ClassUtil.stringConvertClass("floatunsigned"), Float.class);
        Assert.assertEquals(ClassUtil.stringConvertClass("doubleunsigned"), Double.class);
        Assert.assertEquals(ClassUtil.stringConvertClass("date"), Date.class);
        Assert.assertEquals(ClassUtil.stringConvertClass("timestamp"), Timestamp.class);
        Assert.assertEquals(ClassUtil.stringConvertClass("time"), Time.class);
        Assert.assertEquals(ClassUtil.stringConvertClass("decimalunsigned"), BigDecimal.class);
        try {
            ClassUtil.stringConvertClass("other");
        } catch (Exception e) {

        }
    }

    @Test
    public void testConvertType(){
        ClassUtil.convertType(1, "other", "tinyint");
        ClassUtil.convertType(1, "other", "smallint");
        ClassUtil.convertType(1, "other", "int");
        ClassUtil.convertType(1, "other", "bigint");
        ClassUtil.convertType(1, "other", "float");
        ClassUtil.convertType(1, "other", "double");
        ClassUtil.convertType("1", "other", "string");
        Date date = new Date(new java.util.Date().getTime());
        ClassUtil.convertType(date, "other", "date");
        ClassUtil.convertType(date.getTime(), "other", "timestamp");
        try {
            ClassUtil.convertType(1, "other", "sd");
        } catch (Exception e) {

        }
    }

    @Test
    public void testGetTypeFromClass(){
        Assert.assertEquals(ClassUtil.getTypeFromClass(Byte.class), "TINYINT");
        Assert.assertEquals(ClassUtil.getTypeFromClass(Short.class), "SMALLINT");
        Assert.assertEquals(ClassUtil.getTypeFromClass(Integer.class), "INT");
        Assert.assertEquals(ClassUtil.getTypeFromClass(Long.class), "BIGINT");
        Assert.assertEquals(ClassUtil.getTypeFromClass(String.class), "STRING");
        Assert.assertEquals(ClassUtil.getTypeFromClass(Float.class), "FLOAT");
        Assert.assertEquals(ClassUtil.getTypeFromClass(Double.class), "DOUBLE");
        Assert.assertEquals(ClassUtil.getTypeFromClass(Date.class), "DATE");
        Assert.assertEquals(ClassUtil.getTypeFromClass(Timestamp.class), "TIMESTAMP");
        Assert.assertEquals(ClassUtil.getTypeFromClass(Boolean.class), "BOOLEAN");
        try {
            ClassUtil.getTypeFromClass(Object.class);
        } catch (Exception e){

        }
    }
    @Test
    public void testGetTypeFromClassName(){
        Assert.assertEquals(ClassUtil.getTypeFromClassName(Byte.class.getName()), "TINYINT");
        Assert.assertEquals(ClassUtil.getTypeFromClassName(Short.class.getName()), "SMALLINT");
        Assert.assertEquals(ClassUtil.getTypeFromClassName(Integer.class.getName()), "INT");
        Assert.assertEquals(ClassUtil.getTypeFromClassName(Long.class.getName()), "BIGINT");
        Assert.assertEquals(ClassUtil.getTypeFromClassName(String.class.getName()), "STRING");
        Assert.assertEquals(ClassUtil.getTypeFromClassName(Float.class.getName()), "FLOAT");
        Assert.assertEquals(ClassUtil.getTypeFromClassName(Double.class.getName()), "DOUBLE");
        Assert.assertEquals(ClassUtil.getTypeFromClassName(Date.class.getName()), "DATE");
        Assert.assertEquals(ClassUtil.getTypeFromClassName(Timestamp.class.getName()), "TIMESTAMP");
        Assert.assertEquals(ClassUtil.getTypeFromClassName(Boolean.class.getName()), "BOOLEAN");
        try {
            ClassUtil.getTypeFromClassName(Object.class.getName());
        } catch (Exception e){

        }
    }
}
