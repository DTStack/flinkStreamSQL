package com.dtstack.flink.sql.side.rdb.all;

import org.junit.Before;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

/**
 * @program: flinkStreamSQL
 * @author: wuren
 * @create: 2020/11/10
 **/
public abstract class RdbAllReqRowTestBase {

    protected AbstractRdbAllReqRow reqRow;
    protected Class<? extends AbstractRdbAllReqRow> clazz;

    @Before
    public void setUp() {
        init();
        this.reqRow = Whitebox.newInstance(clazz);
    }

    protected abstract void init();

    @Test
    public void testGetConn() {
        try {
            reqRow.getConn("", "", "");
        } catch (RuntimeException e) {}
    }

}
