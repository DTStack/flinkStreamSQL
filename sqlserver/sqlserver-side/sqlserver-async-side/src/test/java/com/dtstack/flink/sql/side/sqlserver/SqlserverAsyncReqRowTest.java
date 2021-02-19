package com.dtstack.flink.sql.side.sqlserver;

import com.dtstack.flink.sql.side.rdb.async.RdbAsyncReqRowTestBase;

public class SqlserverAsyncReqRowTest extends RdbAsyncReqRowTestBase {

    @Override
    protected void init() {
        clazz = SqlserverAsyncReqRow.class;
    }

}