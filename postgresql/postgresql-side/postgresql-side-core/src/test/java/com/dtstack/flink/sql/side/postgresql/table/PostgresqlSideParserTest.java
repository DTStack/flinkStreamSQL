package com.dtstack.flink.sql.side.postgresql.table;


import com.dtstack.flink.sql.table.AbstractTableInfo;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class PostgresqlSideParserTest {

//    @Test
    public void getTableInfo() {
        PostgresqlSideParser sideParser = new PostgresqlSideParser();

        final String tableName = "table_foo";
        final String fieldsInfo = "id INT, name VARCHAR, PRIMARY  KEY  (id)";

        Map<String, Object> props = new HashMap<String, Object>();
        props.put("url", "jdbc:postgresql://foo:3306/db_foo");
        props.put("tablename", "table_foo");
        props.put("username", "foo");
        props.put("password", "foo");

        AbstractTableInfo tableInfo= sideParser.getTableInfo(tableName, fieldsInfo, props);

        final String NORMAL_TYPE = "postgresql";
        final String table_type = tableInfo.getType();
        Assert.assertTrue(NORMAL_TYPE.equals(table_type));
    }

}