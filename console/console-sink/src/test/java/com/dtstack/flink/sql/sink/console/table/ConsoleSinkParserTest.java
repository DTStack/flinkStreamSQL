package com.dtstack.flink.sql.sink.console.table;

import com.dtstack.flink.sql.table.AbstractTableInfo;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ConsoleSinkParserTest {
    
    @Test
    public void getTableInfo() {
        ConsoleSinkParser sinkParser = new ConsoleSinkParser();

        final String tableName = "table_foo";
        final String fieldsInfo = "id INT, name VARCHAR";

        Map<String, Object> props = new HashMap<String, Object>();
        props.put("url", "jdbc:mysql://foo:3306/db_foo");
        props.put("tablename", "table_foo");
        props.put("username", "foo");
        props.put("password", "foo");

        AbstractTableInfo tableInfo= sinkParser.getTableInfo(tableName, fieldsInfo, props);

        final String table_type = tableInfo.getName();
        Assert.assertTrue(tableName.equals(table_type));
    }
    
}