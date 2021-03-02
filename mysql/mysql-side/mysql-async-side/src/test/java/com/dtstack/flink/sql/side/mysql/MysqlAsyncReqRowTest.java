package com.dtstack.flink.sql.side.mysql;

import com.dtstack.flink.sql.side.rdb.async.RdbAsyncReqRowTestBase;

public class MysqlAsyncReqRowTest extends RdbAsyncReqRowTestBase {

    @Override
    protected void init() {
        clazz = MysqlAsyncReqRow.class;
    }

}