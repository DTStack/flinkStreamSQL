package com.dtstack.flink.sql.side.oceanbase;

import com.dtstack.flink.sql.side.rdb.async.RdbAsyncReqRowTestBase;

public class OceanbaseAsyncReqRowTest extends RdbAsyncReqRowTestBase {

    @Override
    protected void init() {
        clazz = OceanbaseAsyncReqRow.class;
    }

}