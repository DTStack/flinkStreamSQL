package com.dtstack.flink.sql.side.rdb.table;

import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.table.RdbParserTestBase;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @program: flinkStreamSQL
 * @author: wuren
 * @create: 2020/11/09
 **/
public class RdbSideParserTest extends RdbParserTestBase {

    @Override
    public void setUp() {
        this.parser = new RdbSideParser();
    }

    public void parpare(RdbSideParser parser, String type) {
        this.parser = parser;
        this.type = type;
    }

    // @Test
    public void testGetTableInfo() {
        RdbSideParser parser = (RdbSideParser) this.parser;
        final String tableName = "table_foo";
        final String fieldsInfo = "id INT, name VARCHAR";

        Map<String, Object> props = new HashMap<String, Object>();
        props.put("url", "jdbc:mysql://foo:3306/db_foo");
        props.put("tablename", "table_foo");
        props.put("username", "foo");
        props.put("password", "foo");
        AbstractTableInfo tableInfo = parser.getTableInfo(tableName, fieldsInfo, props);
        if (this.type != null) {
            final String tableType = tableInfo.getType();
            Assert.assertTrue(this.type.equals(tableType));
        }
    }

}
