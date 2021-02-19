package com.dtstack.flink.sql.side.postgresql;

import com.dtstack.flink.sql.side.rdb.all.RdbAllReqRowTestBase;

public class PostgresqlAllReqRowTest extends RdbAllReqRowTestBase {

    @Override
    protected void init() {
        clazz = PostgresqlAllReqRow.class;
    }

}