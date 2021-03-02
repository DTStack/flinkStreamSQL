package com.dtstack.flink.sql.side.postgresql;

import com.dtstack.flink.sql.side.rdb.async.RdbAsyncReqRowTestBase;

public class PostgresqlAsyncReqRowTest extends RdbAsyncReqRowTestBase {

    @Override
    protected void init() {
        clazz = PostgresqlAsyncReqRow.class;
    }

}