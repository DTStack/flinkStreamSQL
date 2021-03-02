package com.dtstack.flink.sql.side.rdb.table;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RdbSideTableInfoTest {

    private RdbSideTableInfo tableInfo;
    private static String SCHEMA = "TEST_schema";

    @Before
    public void setUp() {
        tableInfo = new RdbSideTableInfo();
        tableInfo.setUrl("jdbc://mysql");
        tableInfo.setUserName("TEST_root");
        tableInfo.setPassword("TEST_pass");
        tableInfo.setTableName("foo_tablename");
        tableInfo.setSchema(SCHEMA);
    }

//    @Test
    public void testCheck() {
        Boolean success = tableInfo.check();
        Assert.assertTrue(success);
    }

//    @Test
    public void testToString() {
        tableInfo.toString();
    }

//    @Test
    public void testGetSchema() {
        Assert.assertEquals(SCHEMA, tableInfo.getSchema());
    }

}