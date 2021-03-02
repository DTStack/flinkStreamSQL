package com.dtstack.flink.sql.sink.rdb.dialect;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.NoSuchElementException;
import java.util.Optional;

public class JDBCDialectTest {

    JDBCDialect dialect;
    final String TABLE_NAME = "TEST_table_name";
    final String[] fieldNames = new String[] {"id", "ct"};
    final String[] conditionFields = new String[] {"id"};
    final String SCHEMA = "schema";
    @Before
    public void setUp() {
        dialect = new ConcreteJDBCDialect();
    }

    @Test
    public void testDefaultDriverName() {
        Optional o = dialect.defaultDriverName();
        try {
            o.get();
        } catch (NoSuchElementException e) { }
    }

    @Test
    public void testQuoteIdentifier() {
        String identifier = dialect.quoteIdentifier("foo");
        String normal = "\"foo\"";
        Assert.assertTrue(normal.equals(identifier));
    }

    @Test
    public void testGetUpsertStatement() {
        Optional o = dialect.getUpsertStatement(
                null, null, null, null, false
            );
        final String normal = "foo";
        Assert.assertTrue(
            o.orElse(normal).equals(normal)
        );
    }

    @Test
    public void testGetRowExistsStatement() {
        String[] conditionFields = new String[] {"id"};
        String stmt = dialect.getRowExistsStatement(TABLE_NAME, conditionFields);
        String normal = "SELECT 1 FROM \"TEST_table_name\" WHERE \"id\"=?";
        Assert.assertTrue(normal.equals(stmt));
    }

    @Test
    public void testGetInsertIntoStatement() {
        String[] partitionFields = new String[] {"ct"};
        String stmt = dialect.getInsertIntoStatement("schema", TABLE_NAME, fieldNames, partitionFields);
        String normal = "INSERT INTO \"schema\".\"TEST_table_name\"(\"id\", \"ct\") VALUES (?, ?)";
        Assert.assertTrue(normal.equals(stmt));
    }

    @Test
    public void testGetUpdateStatement() {
        String stmt = dialect.getUpdateStatement(TABLE_NAME, fieldNames, conditionFields);
        String normal = "UPDATE \"TEST_table_name\" SET \"id\"=?, \"ct\"=? WHERE \"id\"=?";
        Assert.assertTrue(normal.equals(stmt));

    }

    @Test
    public void testGetDeleteStatement() {
        String stmt = dialect.getDeleteStatement(SCHEMA, TABLE_NAME, conditionFields);
        String normal = "DELETE FROM \"schema\".\"TEST_table_name\" WHERE \"id\"=?";
        Assert.assertTrue(normal.equals(stmt));

    }

    @Test
    public void testGetSelectFromStatement() {
        String stmt = dialect.getSelectFromStatement(TABLE_NAME, fieldNames, conditionFields);
        String normal = "SELECT \"id\", \"ct\" FROM \"TEST_table_name\" WHERE \"id\"=?";
        Assert.assertTrue(normal.equals(stmt));
    }
}