package com.dtstack.flink.sql.util;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;

public class MathUtilTest {

    @Test
    public void testGetLongVal() {
        Assert.assertEquals(MathUtil.getLongVal(null), null);
        Assert.assertEquals(MathUtil.getLongVal("12"), Long.valueOf(12));
        Assert.assertEquals(MathUtil.getLongVal(12l), Long.valueOf(12));
        Assert.assertEquals(MathUtil.getLongVal(12), Long.valueOf(12));
        Assert.assertEquals(MathUtil.getLongVal(BigDecimal.valueOf(12)), Long.valueOf(12));
        Assert.assertEquals(MathUtil.getLongVal(BigInteger.valueOf(12)), Long.valueOf(12));
        try {
            MathUtil.getLongVal(new Object());
        } catch (Exception e) {

        }
    }

    @Test
    public void testGetLongValWithDefault() {
        Assert.assertEquals(MathUtil.getLongVal(null, 12l), Long.valueOf(12));
    }

    @Test
    public void testGetIntegerVal() {
        Assert.assertEquals(MathUtil.getIntegerVal(null), null);
        Assert.assertEquals(MathUtil.getIntegerVal("12"), Integer.valueOf(12));
        Assert.assertEquals(MathUtil.getIntegerVal(12l), Integer.valueOf(12));
        Assert.assertEquals(MathUtil.getIntegerVal(12), Integer.valueOf(12));
        Assert.assertEquals(MathUtil.getIntegerVal(BigDecimal.valueOf(12)), Integer.valueOf(12));
        Assert.assertEquals(MathUtil.getIntegerVal(BigInteger.valueOf(12)), Integer.valueOf(12));
        try {
            MathUtil.getIntegerVal(new Object());
        } catch (Exception e) {

        }
    }

    @Test
    public void testGetIntegerValWithDefault() {
        Assert.assertEquals(MathUtil.getIntegerVal(null, 12), Integer.valueOf(12));
    }

    @Test
    public void testGetFloatVal() {
        Assert.assertEquals(MathUtil.getFloatVal(null), null);
        Assert.assertEquals(MathUtil.getFloatVal("12"), Float.valueOf(12));
        Assert.assertEquals(MathUtil.getFloatVal(12.2f), Float.valueOf(12.2f));
        Assert.assertEquals(MathUtil.getFloatVal(BigDecimal.valueOf(12)), Float.valueOf(12));
        try {
            MathUtil.getFloatVal(new Object());
        } catch (Exception e) {

        }
    }

    @Test
    public void testGetFloatValWithDefault() {
        Assert.assertEquals(MathUtil.getFloatVal(null, 12.2f), Float.valueOf(12.2f));
    }

    @Test
    public void testGetDoubleVal() {
        Assert.assertEquals(MathUtil.getDoubleVal(null), null);
        Assert.assertEquals(MathUtil.getDoubleVal("12"), Double.valueOf(12));
        Assert.assertEquals(MathUtil.getDoubleVal(12.3), Double.valueOf(12.3));
        Assert.assertEquals(MathUtil.getDoubleVal(12.2f), Double.valueOf(12.2f));
        Assert.assertEquals(MathUtil.getDoubleVal(BigDecimal.valueOf(12)), Double.valueOf(12));
        Assert.assertEquals(MathUtil.getDoubleVal(12), Double.valueOf(12));
        try {
            MathUtil.getDoubleVal(new Object());
        } catch (Exception e) {

        }
    }

    @Test
    public void testGetDoubleValWithDefault() {
        Assert.assertEquals(MathUtil.getDoubleVal(null, 12.2), Double.valueOf(12.2));
    }

    @Test
    public void testGetBooleanVal() {
        Assert.assertEquals(MathUtil.getBoolean(null), null);
        Assert.assertEquals(MathUtil.getBoolean("true"), true);
        Assert.assertEquals(MathUtil.getBoolean(true), true);
        try {
            MathUtil.getBoolean(new Object());
        } catch (Exception e) {

        }
    }

    @Test
    public void testGetBooleanValWithDefault() {
        Assert.assertEquals(MathUtil.getBoolean(null, true), true);
    }

    @Test
    public void testGetStringVal() {
        Assert.assertEquals(MathUtil.getString(null), null);
        Assert.assertEquals(MathUtil.getString("true"), "true");
        Assert.assertEquals(MathUtil.getString(11), "11");

    }

    @Test
    public void testGetByteVal() {
        Assert.assertEquals(MathUtil.getByte(null), null);
        Assert.assertEquals(MathUtil.getByte("11"), Byte.valueOf("11"));
        Assert.assertEquals(MathUtil.getByte(new Byte("11")), Byte.valueOf("11"));
        try {
            MathUtil.getByte(new Object());
        } catch (Exception e) {

        }

    }

    @Test
    public void testGetShortVal() {
        Assert.assertEquals(MathUtil.getShort(null), null);
        Assert.assertEquals(MathUtil.getShort("11"), Short.valueOf("11"));
        Assert.assertEquals(MathUtil.getShort(new Short((short) 11)), new Short((short) 11));
        try {
            MathUtil.getShort(new Object());
        } catch (Exception e) {

        }

    }

    @Test
    public void testGetDecimalVal() {
        Assert.assertEquals(MathUtil.getBigDecimal(null), null);
        Assert.assertEquals(MathUtil.getBigDecimal("11"), new BigDecimal("11"));
        Assert.assertEquals(MathUtil.getBigDecimal(new BigDecimal("11")), new BigDecimal("11"));
        Assert.assertEquals(MathUtil.getBigDecimal(new BigInteger("11")), new BigDecimal("11"));
        try {
            MathUtil.getBigDecimal(new Object());
        } catch (Exception e) {

        }
    }

    @Test
    public void testGetDateVal() {
        Date now = DateUtil.getDateFromStr("2020-07-12");
        Assert.assertEquals(MathUtil.getDate(null), null);
        Assert.assertEquals(MathUtil.getDate("2020-07-12"), now);
        Assert.assertEquals(MathUtil.getDate(new Timestamp(now.getTime())), now);
        Assert.assertEquals(MathUtil.getDate(now), now);
        try {
            MathUtil.getDate(new Object());
        } catch (Exception e) {

        }
    }
    @Test
    public void testGetTimestampVal() {
        Date now = DateUtil.getDateFromStr("2020-07-12");
        Assert.assertEquals(MathUtil.getTimestamp(null), null);
        Assert.assertEquals(MathUtil.getTimestamp("2020-07-12"), new Timestamp(now.getTime()));
        Assert.assertEquals(MathUtil.getTimestamp(new Timestamp(now.getTime())), new Timestamp(now.getTime()));
        Assert.assertEquals(MathUtil.getTimestamp(now), new Timestamp(now.getTime()));
        try {
            MathUtil.getTimestamp(new Object());
        } catch (Exception e) {

        }
    }

}
