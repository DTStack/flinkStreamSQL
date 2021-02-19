package com.dtstack.flink.sql.sink.rdb;

import com.dtstack.flink.sql.sink.rdb.dialect.ConcreteJDBCDialect;
import com.dtstack.flink.sql.sink.rdb.dialect.JDBCDialect;
import org.junit.Assert;
import org.junit.Test;

public class JDBCOptionsTest {

    @Test
    public void testBuilder() {
        JDBCOptions.Builder builder = JDBCOptions.builder();
        final String diverName = "org.postgresql.Driver";
        final String dbUrl = "jdbc:postgres://";
        final String tableName = "TEST_tablename";
        final String username = "TEST_user";
        final String password = "TEST_pass";
        final String schema = "TEST_schema";

        try {
            JDBCOptions options = builder
               .setDbUrl(dbUrl)
               .setUsername(username)
               .setPassword(password)
               .setTableName(tableName)
               .setDialect(new ConcreteJDBCDialect())
               .setSchema(schema).build();
        } catch (NullPointerException e) {
           Assert.assertEquals(e.getMessage(), "No driverName supplied.");
        }

        final JDBCDialect dialect = new ConcreteJDBCDialect();
        JDBCOptions options = builder
            .setDriverName(diverName)
            .setDbUrl(dbUrl)
            .setUsername(username)
            .setPassword(password)
            .setTableName(tableName)
            .setDialect(dialect)
            .setSchema(schema).build();

        Assert.assertEquals(options, options);
        Assert.assertNotEquals(options, null);
        Assert.assertEquals(options.getDriverName(), diverName);
        Assert.assertEquals(options.getDbUrl(), dbUrl);
        Assert.assertEquals(options.getUsername(), username);
        Assert.assertEquals(options.getPassword(), password);
        Assert.assertEquals(options.getTableName(), tableName);
        Assert.assertEquals(options.getDialect(), dialect);
        Assert.assertEquals(options.getSchema(), schema);
    }

}