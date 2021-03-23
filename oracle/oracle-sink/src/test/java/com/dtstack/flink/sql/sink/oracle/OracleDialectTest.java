package com.dtstack.flink.sql.sink.oracle;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;

public class OracleDialectTest {

    OracleDialect dialect;

    @Before
    public void setUp() throws Exception {
        dialect = new OracleDialect();
        ArrayList<String> fields = new ArrayList<>();
        fields.add("id");
        fields.add("name");
        dialect.setFieldList(fields);
        ArrayList<String> fieldTypes = new ArrayList<>();
        fieldTypes.add("INT");
        fieldTypes.add("STRING");
        dialect.setFieldTypeList(fieldTypes);
//        dialect.setFieldExtraInfoList();
    }

    @Test
    public void testEasyUtils() {
        final String s = "jdbc:oracle://localhost:3306/foo_db";
        boolean r = dialect.canHandle(s);
        Assert.assertTrue(r);

        String driver = dialect.defaultDriverName().get();
        Assert.assertTrue(driver.equals("oracle.jdbc.driver.OracleDriver"));
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
                "MERGE INTO \"table_foo\" " +
                "T1 USING (SELECT  ? \"id\",  ? \"name\" FROM DUAL) " +
                "T2 ON (T1.\"id\"=T2.\"id\")  " +
                "WHEN MATCHED THEN UPDATE SET \"T1\".\"name\" =nvl(\"T2\".\"name\",\"T1\".\"name\") " +
                "WHEN NOT MATCHED THEN INSERT (\"id\",\"name\") VALUES (T2.\"id\",T2.\"name\")";

        String upsertStmt = dialect
                .getUpsertStatement("", tableName, fieldNames, uniqueKeyFields, false)
                .get();

        Assert.assertTrue(NORMAL_STMT.equals(upsertStmt));
    }

}