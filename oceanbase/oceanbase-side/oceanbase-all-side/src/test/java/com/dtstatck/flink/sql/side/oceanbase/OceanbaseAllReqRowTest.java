package com.dtstatck.flink.sql.side.oceanbase;

import com.dtstack.flink.sql.side.oceanbase.OceanbaseAllReqRow;
import com.dtstack.flink.sql.side.rdb.all.RdbAllReqRowTestBase;

public class OceanbaseAllReqRowTest extends RdbAllReqRowTestBase {

    @Override
    protected void init() {
        clazz = OceanbaseAllReqRow.class;
    }

}