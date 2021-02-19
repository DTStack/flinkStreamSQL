package com.dtstack.flink.sql.side.kingbase;

import com.dtstack.flink.sql.side.rdb.async.RdbAsyncReqRowTestBase;

public class KingbaseAsyncReqRowTest extends RdbAsyncReqRowTestBase {

    @Override
    protected void init() {
        clazz = KingbaseAsyncReqRow.class;
    }

}