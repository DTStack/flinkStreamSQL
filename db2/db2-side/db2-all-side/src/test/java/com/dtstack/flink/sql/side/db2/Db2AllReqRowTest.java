package com.dtstack.flink.sql.side.db2;

import com.dtstack.flink.sql.side.rdb.all.RdbAllReqRowTestBase;

public class Db2AllReqRowTest extends RdbAllReqRowTestBase {

    @Override
    protected void init() {
        clazz = Db2AllReqRow.class;
    }

}