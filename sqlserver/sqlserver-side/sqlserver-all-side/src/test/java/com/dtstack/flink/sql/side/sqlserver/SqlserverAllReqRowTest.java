package com.dtstack.flink.sql.side.sqlserver;

import com.dtstack.flink.sql.side.rdb.all.RdbAllReqRowTestBase;

public class SqlserverAllReqRowTest extends RdbAllReqRowTestBase {

    @Override
    protected void init() {
        clazz = SqlserverAllReqRow.class;
    }

}