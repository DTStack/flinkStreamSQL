package com.dtstack.flink.sql.side.clickhouse;

import com.dtstack.flink.sql.side.rdb.async.RdbAsyncReqRowTestBase;

public class ClickhouseAsyncReqRowTest extends RdbAsyncReqRowTestBase {

    @Override
    protected void init() {
        clazz = ClickhouseAsyncReqRow.class;
    }

}