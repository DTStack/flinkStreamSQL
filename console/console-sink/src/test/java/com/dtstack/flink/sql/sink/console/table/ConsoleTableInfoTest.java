package com.dtstack.flink.sql.sink.console.table;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @program: flink.sql
 * @description:
 * @author: wuren
 * @create: 2020-06-16 20:18
 **/
public class ConsoleTableInfoTest {
    private ConsoleTableInfo consoleTableInfo;
    @Before
    public void setUp() {
        consoleTableInfo = new ConsoleTableInfo();
    }

    @Test
    public void testCheck() {
        Boolean b = consoleTableInfo.check();
        Assert.assertTrue(b == true);
    }

    @Test
    public void testGetType() {
        String r = consoleTableInfo.getType();
        Assert.assertTrue("console".equals(r));
    }
}
