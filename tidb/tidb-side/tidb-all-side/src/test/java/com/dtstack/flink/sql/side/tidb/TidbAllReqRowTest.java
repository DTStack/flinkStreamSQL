package com.dtstack.flink.sql.side.tidb;

import com.dtstack.flink.sql.side.rdb.all.RdbAllReqRowTestBase;

public class TidbAllReqRowTest extends RdbAllReqRowTestBase {

    @Override
    protected void init() {
        clazz = TidbAllReqRow.class;
    }

}