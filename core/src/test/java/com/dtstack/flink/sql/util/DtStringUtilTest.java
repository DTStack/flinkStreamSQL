package com.dtstack.flink.sql.util;

import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;

public class DtStringUtilTest {

    @Test
    public void testSplitIgnoreQuota(){
        DtStringUtil.splitIgnoreQuota("(ss, aa)", ',');
    }

    @Test
    public void testReplaceIgnoreQuota(){
        String str = DtStringUtil.replaceIgnoreQuota("abcdfs", "s", "a");
        Assert.assertEquals(str, "abcdfa");

    }

    @Test
    public void testCol2string(){
        Assert.assertEquals(DtStringUtil.col2string(1, "TINYINT"), "1");
        Assert.assertEquals(DtStringUtil.col2string(1, "SMALLINT"), "1");
        Assert.assertEquals(DtStringUtil.col2string(1, "INT"), "1");
        Assert.assertEquals(DtStringUtil.col2string(1, "BIGINT"), "1");
        Assert.assertEquals(DtStringUtil.col2string(1, "FLOAT"), Float.valueOf(1).toString());
        Assert.assertEquals(DtStringUtil.col2string(1, "DOUBLE"), Double.valueOf(1).toString());
        Assert.assertEquals(DtStringUtil.col2string(1, "DECIMAL"), new BigDecimal(1).toString());
        Assert.assertEquals(DtStringUtil.col2string(1, "CHAR"), "1");
        Assert.assertEquals(DtStringUtil.col2string(true, "BOOLEAN"), Boolean.valueOf(true).toString());
        Assert.assertEquals(DtStringUtil.col2string(new Date(), "DATE"), DateUtil.dateToString(new Date()));
        Assert.assertEquals(DtStringUtil.col2string(new Date(), "TIMESTAMP"), DateUtil.timestampToString(new Date()));
        try {
            Assert.assertEquals(DtStringUtil.col2string(1, "other"), "1");
        } catch (Exception e){

        }
    }

    @Test
    public void testGetPluginTypeWithoutVersion(){
        Assert.assertEquals(DtStringUtil.getPluginTypeWithoutVersion("kafka10"), "kafka");
    }

    @Test
    public void testAddJdbcParam(){
        try {
            DtStringUtil.addJdbcParam(null, null, false);
        }catch (Exception e){

        }
        DtStringUtil.addJdbcParam("ss", null, false);
        Map<String, String> addParams = Maps.newHashMap();
        addParams.put("aa", "bb");
        DtStringUtil.addJdbcParam("jdbc:mysql://172.16.8.104:3306/test?charset=utf8", addParams, false);
    }

    @Test
    public void testIsJson(){
        Assert.assertEquals(DtStringUtil.isJson("{}"), true);
        try {
            DtStringUtil.isJson("");
        }catch (Exception e){

        }
    }

    @Test
    public void testParse(){
        Assert.assertEquals(DtStringUtil.parse("1", Integer.class), 1);
        Assert.assertEquals(DtStringUtil.parse("1", Long.class), Long.valueOf(1));
        Assert.assertEquals(DtStringUtil.parse("1", Byte.class), "1".getBytes()[0]);
        Assert.assertEquals(DtStringUtil.parse("1", String.class), "1");
        Assert.assertEquals(DtStringUtil.parse("1", Float.class), Float.valueOf(1));
        Assert.assertEquals(DtStringUtil.parse("1", Double.class), Double.valueOf(1));
        Assert.assertEquals(DtStringUtil.parse("2020-02-03 11:33:33", Timestamp.class), Timestamp.valueOf("2020-02-03 11:33:33"));
        try {
            Assert.assertEquals(DtStringUtil.parse("1", Object.class), 1);
        } catch (Exception e){

        }
    }

    @Test
    public void testFirstUpperCase(){
        Assert.assertEquals(DtStringUtil.firstUpperCase("abc"), "Abc");;
    }

    @Test
    public void testGetTableFullPath(){
        Assert.assertEquals(DtStringUtil.getTableFullPath("aa", "aa.roc"), "\"aa\".\"roc\"");
    }


}
