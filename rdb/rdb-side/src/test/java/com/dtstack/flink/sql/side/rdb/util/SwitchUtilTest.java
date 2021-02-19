package com.dtstack.flink.sql.side.rdb.util;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;

public class SwitchUtilTest {

    private int INT_NUMBER = 1;

    @Test
    public void testGetTargetInt() {
        Object o = SwitchUtil.getTarget(INT_NUMBER, "INT");
        Assert.assertTrue((Integer) o == INT_NUMBER);
    }

    @Test
    public void testGetTargetLong() {
        Object o = SwitchUtil.getTarget(INT_NUMBER, "IntegerUnsigned");
        Assert.assertTrue((Long) o == INT_NUMBER);
    }

    @Test
    public void testGetTargetBoolean() {
        Object o = SwitchUtil.getTarget(true, "Boolean");
        Assert.assertTrue((Boolean) o);
    }

    @Test
    public void testGetTargetBlob() {
        byte b = 65;
        Object o = SwitchUtil.getTarget(b, "Blob");
        System.out.println(o);
        Assert.assertTrue(((Byte) o) == b);
    }

    @Test
    public void testGetTargetString() {
        Object o = SwitchUtil.getTarget(INT_NUMBER, "Text");
        Assert.assertTrue(
            ((String) o).equals(String.valueOf(INT_NUMBER))
        );
    }

    @Test
    public void testGetTargetFloat() {
        Object o = SwitchUtil.getTarget(String.valueOf(INT_NUMBER), "FloatUnsigned");
        Assert.assertTrue((Float) o == 1.0);
    }

    @Test
    public void testGetTargetDouble() {
        Object o = SwitchUtil.getTarget(String.valueOf(INT_NUMBER), "DoubleUnsigned");
        Assert.assertTrue((Double) o == 1.0);
    }

    @Test
    public void testGetTargetDecimal() {
        Object o = SwitchUtil.getTarget(String.valueOf(INT_NUMBER), "DecimalUnsigned");
        Assert.assertTrue(
            ((BigDecimal) o).equals(BigDecimal.valueOf(INT_NUMBER))
        );
    }

    @Test
    public void testGetTargeDate() {
        String dateString = "2020-02-02";
        Date date = (Date) SwitchUtil.getTarget("2020-02-02", "Date");
        Assert.assertTrue(
            dateString.equals(date.toString())
        );
    }

    @Test
    public void testGetTargetTimestamp() {
        String timestampStr = "2020-02-02 12:12:12";
        Timestamp timestamp = (Timestamp) SwitchUtil.getTarget(timestampStr, "DateTime");
        Assert.assertTrue(
            (timestampStr + ".0").equals(timestamp.toString())
        );
    }

    @Test
    public void testDefault() {
        String normal = "TEST";
        String str = (String) SwitchUtil.getTarget(normal, "NONE");
        Assert.assertEquals(normal, str);
    }
}