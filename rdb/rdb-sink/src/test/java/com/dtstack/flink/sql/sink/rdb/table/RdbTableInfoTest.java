package com.dtstack.flink.sql.sink.rdb.table;

import org.junit.Assert;
import org.junit.Test;

public class RdbTableInfoTest {

    @Test
    public void test() {
        RdbTableInfo tablaInfo = new RdbTableInfo();
        final String type = "mysql";
        tablaInfo.setType(type);
        Assert.assertEquals(type, tablaInfo.getType());
    }

}