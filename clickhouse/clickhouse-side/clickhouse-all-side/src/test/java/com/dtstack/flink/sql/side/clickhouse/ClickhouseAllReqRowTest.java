package com.dtstack.flink.sql.side.clickhouse;

import com.dtstack.flink.sql.side.rdb.all.RdbAllReqRowTestBase;

public class ClickhouseAllReqRowTest extends RdbAllReqRowTestBase {

    @Override
    protected void init() {
        clazz = ClickhouseAllReqRow.class;
    }

}