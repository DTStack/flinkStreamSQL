package com.dtstack.flink.sql.side.db2;

import com.dtstack.flink.sql.side.rdb.async.RdbAsyncReqRowTestBase;

public class Db2AsyncReqRowTest extends RdbAsyncReqRowTestBase {

    @Override
    protected void init() {
        clazz = Db2AsyncReqRow.class;
    }

}