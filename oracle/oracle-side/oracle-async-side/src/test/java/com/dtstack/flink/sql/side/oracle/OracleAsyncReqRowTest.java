package com.dtstack.flink.sql.side.oracle;

import com.dtstack.flink.sql.side.rdb.async.RdbAsyncReqRowTestBase;

public class OracleAsyncReqRowTest extends RdbAsyncReqRowTestBase {

    @Override
    protected void init() {
        clazz = OracleAsyncReqRow.class;
    }

}