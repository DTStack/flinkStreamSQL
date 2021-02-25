package com.dtstack.flink.sql.sink.impala.table;

import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class ImpalaSinkParserTest {

    ImpalaSinkParser sinkParser;

    @Before
    public void setUp() {
        sinkParser = new ImpalaSinkParser();
    }

//    @Test
    public void testGetTableInfo() throws Exception {

        final String tableName = "table_foo";
        final String fieldsInfo = "id INT, name VARCHAR";

        Map<String, Object> props = new HashMap<String, Object>();
        props.put("authmech", 3);

        props.put("enablepartition", "true");
        props.put("partitionvalues", "{\"name\":[\"tom\",\"jeck\"]}");
        props.put("partitionfields", "name");
        props.put("partitionfieldtypes", "varchar");

        props.put("url", "jdbc:hive2://myhost.example.com:21050/;principal=impala/myhost.example.com@H2.EXAMPLE.COM");
        props.put("tablename", "table_foo");
        props.put("username", "foo");
        props.put("password", "foo");

        AbstractTableInfo tableInfo= sinkParser.getTableInfo(tableName, fieldsInfo, props);
        props.put("authmech", 2);
        sinkParser.getTableInfo(tableName, fieldsInfo, props);
        props.put("authmech", 1);
        props.put("principal", "");
        props.put("keytabfilepath", "/foo/bar.keytab");
        props.put("krb5filepath", "krb5.conf");
        props.put("krbhostfqdn", "");
        props.put("krbservicename", "");
        sinkParser.getTableInfo(tableName, fieldsInfo, props);
        props.put("authmech", -1);
        try {
            sinkParser.getTableInfo(tableName, fieldsInfo, props);
        } catch (IllegalArgumentException e) { }


//        final String NORMAL_TYPE = "mysql";
//        final String table_type = tableInfo.getType();
//        Assert.assertTrue(NORMAL_TYPE.equals(table_type));
    }

//    @Test
    public void testDbTypeConvertToJavaType() {
        String ERR_TYPE = "TEST_foo";
        try {
            sinkParser.dbTypeConvertToJavaType(ERR_TYPE);
        } catch (Exception e) {
            String normal = "不支持 " + ERR_TYPE +" 类型";
            Assert.assertTrue(e.getMessage().equals(normal));
        }
        Class<?> clazz = sinkParser.dbTypeConvertToJavaType("timestamp");
        Assert.assertTrue(Timestamp.class.equals(clazz));
    }

}