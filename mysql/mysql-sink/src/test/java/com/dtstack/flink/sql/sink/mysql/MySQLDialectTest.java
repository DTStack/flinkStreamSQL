package com.dtstack.flink.sql.sink.mysql;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @program: flink.sql
 * @author: wuren
 * @create: 2020/06/17
 **/
public class MySQLDialectTest {

    MySQLDialect mysqlDialect;

    @Before
    public void setUp() throws Exception {
        mysqlDialect = new MySQLDialect();
    }

    @Test
    public void testEasyUtils() {
        final String s = "jdbc:mysql://localhost:3306/foo_db";
        boolean r = mysqlDialect.canHandle(s);
        Assert.assertTrue(r);

        String driver = mysqlDialect.defaultDriverName().get();
        Assert.assertTrue(driver.equals("com.mysql.jdbc.Driver"));

        final String foo = "foo";
        final String NORMAL_QUOTE = "`foo`";
        String strWithQuote = mysqlDialect.quoteIdentifier(foo);

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
                "REPLACE INTO `table_foo`(`id`, `name`) VALUES (?, ?)";
        final String NORMAL_UPSERT_STMT =
                "INSERT INTO `table_foo`(`id`, `name`) VALUES (?, ?) " +
                        "ON DUPLICATE KEY UPDATE `id`=IFNULL(VALUES(`id`),`id`), " +
                        "`name`=IFNULL(VALUES(`name`),`name`)";

        String replaceStmt = mysqlDialect.buildReplaceIntoStatement(tableName, fieldNames)
                .get();
        String upsertStmt = mysqlDialect.buildDuplicateUpsertStatement(tableName, fieldNames)
                .get();

        Assert.assertTrue(NORMAL_REPELACE_STMT.equals(replaceStmt));
        Assert.assertTrue(NORMAL_UPSERT_STMT.equals(upsertStmt));

        String upsertStmtWithReplace = mysqlDialect
                .getUpsertStatement("", tableName, fieldNames, null, true)
                .get();
        String upsertStmtWithoutReplace = mysqlDialect
                .getUpsertStatement("", tableName, fieldNames, null, false)
                .get();

        Assert.assertTrue(replaceStmt.equals(upsertStmtWithReplace));
        Assert.assertTrue(upsertStmt.equals(upsertStmtWithoutReplace));
    }

}