package com.dtstack.flink.sql.sink.ocean;

import com.dtstack.flink.sql.sink.oceanbase.OceanbaseDialect;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OceanbaseDialectTest {

    OceanbaseDialect dialect;

    @Before
    public void setUp() {
        dialect = new OceanbaseDialect();
    }

    @Test
    public void testEasyUtils() {
        final String s = "jdbc:mysql://localhost:3306/foo_db";
        boolean r = dialect.canHandle(s);
        Assert.assertTrue(r);

        String driver = dialect.defaultDriverName().get();
        Assert.assertTrue(driver.equals("com.mysql.jdbc.Driver"));

        final String foo = "foo";
        final String NORMAL_QUOTE = "`foo`";
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
        final String NORMAL_REPELACE_STMT =
            "REPLACE INTO `table_foo`(`id`,`name`) VALUES (?,?)";
        final String NORMAL_UPSERT_STMT =
            "INSERT INTO `table_foo`(`id`, `name`) VALUES (?, ?) ON DUPLICATE KEY UPDATE `id`IFNULL(VALUES(`id`),`id`),`name`IFNULL(VALUES(`name`),`name`)";

        String upsertStmtWithReplace = dialect
            .getUpsertStatement("", tableName, fieldNames, null, true)
            .get();
        String upsertStmtWithoutReplace = dialect
            .getUpsertStatement("", tableName, fieldNames, null, false)
            .get();
        //TODO 待修复
//        Assert.assertTrue(NORMAL_REPELACE_STMT.equals(upsertStmtWithReplace));
//        Assert.assertTrue(NORMAL_UPSERT_STMT.equals(upsertStmtWithoutReplace));
    }

}