package com.dtstack.flink.sql.sink.rdb.table;

import com.dtstack.flink.sql.table.RdbParserTestBase;
import org.apache.commons.compress.utils.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @program: flinkStreamSQL
 * @author: wuren
 * @create: 2020/11/09
 **/
public class RdbSinkParserTest extends RdbParserTestBase {

    @Before
    public void setUp() {
        this.parser = new RdbSinkParser();
    }

    @Test
    public void testGetTableInfo() {
        RdbSinkParser parser = (RdbSinkParser) this.parser;

        final String tableName = "table_foo";
        final String fieldsInfo = "id INT, name VARCHAR";

        Map<String, Object> props = new HashMap<String, Object>();
        props.put("url", "jdbc:mysql://foo:3306/db_foo");
        props.put("tablename", "table_foo");
        props.put("username", "foo");
        props.put("password", "foo");
        props.put("batchsize", "100");

        RdbTableInfo tableInfo = (RdbTableInfo) parser.getTableInfo(tableName, fieldsInfo, props);
        List<String> pk = Lists.newArrayList();
        pk.add("id");
        tableInfo.setUpdateMode("UPSERT");
        tableInfo.setPrimaryKeys(pk);
        Assert.assertTrue(tableInfo.check());
        if (this.type != null) {
            final String tableType = tableInfo.getType();
            Assert.assertTrue(this.type.equals(tableType));
        }
    }

}
