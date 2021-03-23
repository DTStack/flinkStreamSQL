package com.dtstack.flink.sql.side.mysql;

import com.dtstack.flink.sql.side.rdb.all.RdbAllReqRowTestBase;
import org.junit.Assert;
import org.junit.Test;

public class MysqlAllReqRowTest extends RdbAllReqRowTestBase {

    @Override
    protected void init() {
        clazz = MysqlAllReqRow.class;
    }

    @Test
    public void testFetch() {
        Assert.assertTrue(Integer.MIN_VALUE == reqRow.getFetchSize());
    }

}