package com.dtstack.flink.sql.sink.sqlserver;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SqlserverDialectTest {

    SqlserverDialect dialect;

    @Before
    public void setUp() throws Exception {
        dialect = new SqlserverDialect();
    }

    @Test
    public void testEasyUtils() {
        final String s = "jdbc:jtds://localhost:3306/foo_db";
        boolean r = dialect.canHandle(s);
        Assert.assertTrue(r);

        String driver = dialect.defaultDriverName().get();
        Assert.assertTrue(driver.equals("net.sourceforge.jtds.jdbc.Driver"));
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
                "MERGE INTO \"table_foo\" T1 USING " +
                "(SELECT  ? \"id\",  ? \"name\") T2 ON (T1.\"id\"=T2.\"id\")  " +
                "WHEN MATCHED THEN UPDATE SET \"T1\".\"name\" =ISNULL(\"T2\".\"name\",\"T1\".\"name\") " +
                "WHEN NOT MATCHED THEN INSERT (\"id\",\"name\") VALUES (T2.\"id\",T2.\"name\");";


        String upsertStmt = dialect
                .getUpsertStatement("", tableName, fieldNames, uniqueKeyFields, false)
                .get();

        Assert.assertTrue(NORMAL_STMT.equals(upsertStmt));

    }

}