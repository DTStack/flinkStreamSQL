package com.dtstack.flink.sql.side.polardb;

import com.dtstack.flink.sql.side.rdb.async.RdbAsyncReqRowTestBase;
import org.junit.Before;
import org.powermock.reflect.Whitebox;

public class PolardbAsyncReqRowTest extends RdbAsyncReqRowTestBase {

    @Override
    protected void init() {
        clazz = PolardbAsyncReqRow.class;
    }

}