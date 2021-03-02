package com.dtstack.flink.sql.side.tidb;

import com.dtstack.flink.sql.side.rdb.async.RdbAsyncReqRowTestBase;

public class TidbAsyncReqRowTest extends RdbAsyncReqRowTestBase {

    @Override
    protected void init() {
        clazz = TidbAsyncReqRow.class;
    }

}