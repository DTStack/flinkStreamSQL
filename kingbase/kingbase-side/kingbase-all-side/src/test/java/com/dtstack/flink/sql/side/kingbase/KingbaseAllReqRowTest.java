package com.dtstack.flink.sql.side.kingbase;

import com.dtstack.flink.sql.side.rdb.all.RdbAllReqRowTestBase;

public class KingbaseAllReqRowTest extends RdbAllReqRowTestBase {

    @Override
    protected void init() {
        clazz = KingbaseAllReqRow.class;
    }

}