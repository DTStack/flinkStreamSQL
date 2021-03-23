package com.dtstack.flink.sql.sink.clickhouse.table;

import com.dtstack.flink.sql.table.AbstractTableInfo;
import org.junit.Assert;
import org.junit.Test;
import java.util.HashMap;
import java.util.Map;

public class ClickhouseSinkParserTest {

    @Test
    public void getTableInfo() {
        ClickhouseSinkParser mysqlSinkParser = new ClickhouseSinkParser();

        final String tableName = "table_foo";
        final String fieldsInfo = "id INT, name VARCHAR";

        Map<String, Object> props = new HashMap<String, Object>();
        props.put("url", "jdbc:mysql://foo:3306/db_foo");
        props.put("tablename", "table_foo");
        props.put("username", "foo");
        props.put("password", "foo");

        AbstractTableInfo tableInfo= mysqlSinkParser.getTableInfo(tableName, fieldsInfo, props);

        final String NORMAL_TYPE = "clickhouse";
        final String table_type = tableInfo.getType();
        Assert.assertTrue(NORMAL_TYPE.equals(table_type));
    }

}