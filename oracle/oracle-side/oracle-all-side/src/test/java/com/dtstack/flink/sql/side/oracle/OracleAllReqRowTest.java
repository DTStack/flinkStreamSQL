package com.dtstack.flink.sql.side.oracle;

import com.dtstack.flink.sql.side.rdb.all.RdbAllReqRowTestBase;
import org.junit.Before;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import static org.junit.Assert.*;

public class OracleAllReqRowTest extends RdbAllReqRowTestBase {

    @Override
    protected void init() {
        clazz = OracleAllReqRow.class;
    }
    
}