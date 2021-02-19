package com.dtstack.flink.sql.side.polardb;

import com.dtstack.flink.sql.side.rdb.all.RdbAllReqRowTestBase;

public class PolardbAllReqRowTest extends RdbAllReqRowTestBase {

    @Override
    protected void init() {
        clazz = PolardbAllReqRow.class;
    }

}