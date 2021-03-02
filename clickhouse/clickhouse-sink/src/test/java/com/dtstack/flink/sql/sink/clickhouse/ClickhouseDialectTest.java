package com.dtstack.flink.sql.sink.clickhouse;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ClickhouseDialectTest {
    
    ClickhouseDialect dialect;

    @Before
    public void setUp() throws Exception {
        dialect = new ClickhouseDialect();
    }

    @Test
    public void testEasyUtils() {
        final String s = "jdbc:clickhouse://localhost:3306/foo_db";
        boolean r = dialect.canHandle(s);
        Assert.assertTrue(r);

        String driver = dialect.defaultDriverName().get();
        Assert.assertTrue(driver.equals("ru.yandex.clickhouse.ClickHouseDriver"));
    }

}