package com.dtstack.flink.sql.sink.db;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DbDialectTest {
    
    DbDialect dialect;

    @Before
    public void setUp() throws Exception {
        dialect = new DbDialect();
    }

    @Test
    public void testEasyUtils() {
        final String s = "jdbc:db2://localhost:3306/foo_db";
        boolean r = dialect.canHandle(s);
        Assert.assertTrue(r);

        String driver = dialect.defaultDriverName().get();
        Assert.assertTrue(driver.equals("com.ibm.db2.jcc.DB2Driver"));

        final String foo = "foo";
        final String NORMAL_QUOTE = "foo";
        String strWithQuote = dialect.quoteIdentifier(foo);
        Assert.assertTrue(strWithQuote.equals(NORMAL_QUOTE));
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
                "MERGE INTO \"table_foo\" T1 USING (VALUES( ? ,  ? )) T2 (id, name) ON (T1.id=T2.id)  " +
                "WHEN MATCHED THEN UPDATE SET T1.name =NVL(T2.name,T1.name) " +
                "WHEN NOT MATCHED THEN INSERT (id,name) VALUES (T2.id,T2.name)";

        String upsertStmt = dialect
                .getUpsertStatement("", tableName, fieldNames, uniqueKeyFields, false)
                .get();

        Assert.assertTrue(NORMAL_STMT.equals(upsertStmt));
    }
    
}