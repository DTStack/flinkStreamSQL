package com.dtstack.flink.sql.side.impala;

import com.dtstack.flink.sql.side.impala.table.ImpalaSideParser;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.util.HashMap;
import java.util.Map;

public class ImpalaAsyncSideInfoTest {

    ImpalaAsyncSideInfo sideInfo;
    AbstractTableInfo tableInfo;

    @Before
    public void setUp() {
        sideInfo = Whitebox.newInstance(ImpalaAsyncSideInfo.class);
        final String tableName = "table_foo";
        final String fieldsInfo = "id INT, name VARCHAR, PRIMARY  KEY  (id)";

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

        ImpalaSideParser sinkParser = new ImpalaSideParser();
        tableInfo = sinkParser.getTableInfo(tableName, fieldsInfo, props);
        Whitebox.setInternalState(sideInfo, "sideTableInfo", tableInfo);

    }

    @Test
    public void testGetAdditionalWhereClause() {
        String condition = sideInfo.getAdditionalWhereClause();
        String normal = " AND name IN ('tom' , 'jeck') ";
        Assert.assertTrue(normal.equals(condition));
    }

}