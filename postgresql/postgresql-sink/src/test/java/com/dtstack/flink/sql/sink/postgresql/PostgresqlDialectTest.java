package com.dtstack.flink.sql.sink.postgresql;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class PostgresqlDialectTest {

    PostgresqlDialect dialect;

    @Before
    public void setUp() throws Exception {
        dialect = new PostgresqlDialect();
    }

    @Test
    public void testEasyUtils() {
        final String s = "jdbc:postgresql://localhost:3306/foo_db";
        boolean r = dialect.canHandle(s);
        Assert.assertTrue(r);

        String driver = dialect.defaultDriverName().get();
        Assert.assertTrue(driver.equals("org.postgresql.Driver"));
    }

    @Test
    public void testDialect() {
        final String tableName = "table_foo";
        final String[] fieldNames = new String[] {
                "id",
                "name"
        };
        final String[] uniqueKeyFields = new String[] {
                "id",
        };
        final String NORMAL_STMT =
                "INSERT INTO \"table_foo\"(\"id\", \"name\") VALUES (?, ?) " +
                "ON CONFLICT (\"id\") " +
                "DO UPDATE SET \"id\"=EXCLUDED.\"id\", \"name\"=EXCLUDED.\"name\"";

        String upsertStmt = dialect
                .getUpsertStatement("", tableName, fieldNames, uniqueKeyFields, false)
                .get();

        Assert.assertTrue(NORMAL_STMT.equals(upsertStmt));
    }

}