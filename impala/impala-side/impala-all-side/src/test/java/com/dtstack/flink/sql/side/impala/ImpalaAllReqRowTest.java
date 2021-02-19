package com.dtstack.flink.sql.side.impala;

import com.dtstack.flink.sql.side.impala.table.ImpalaSideParser;
import com.dtstack.flink.sql.side.rdb.all.RdbAllReqRowTestBase;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import org.junit.Before;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ImpalaAllReqRowTest extends RdbAllReqRowTestBase {

    @Override
    protected void init() {
        clazz = ImpalaAllReqRow.class;
    }

    @Test
    public void testGetUrl() throws IOException {
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

        ImpalaAllReqRow rq = (ImpalaAllReqRow) reqRow;
        ImpalaSideParser sinkParser = new ImpalaSideParser();
        AbstractTableInfo tableInfo = sinkParser.getTableInfo(tableName, fieldsInfo, props);
        Whitebox.setInternalState(reqRow, "impalaSideTableInfo", tableInfo);
        rq.getUrl();

        props.put("authmech", 2);
        tableInfo = sinkParser.getTableInfo(tableName, fieldsInfo, props);
        Whitebox.setInternalState(reqRow, "impalaSideTableInfo", tableInfo);
        rq.getUrl();

        props.put("authmech", 1);
        props.put("principal", "");
        props.put("keytabfilepath", "/foo/bar.keytab");
        props.put("krb5filepath", "krb5.conf");
        props.put("krbhostfqdn", "");
        props.put("krbservicename", "");
        tableInfo = sinkParser.getTableInfo(tableName, fieldsInfo, props);
        Whitebox.setInternalState(reqRow, "impalaSideTableInfo", tableInfo);
        rq.getUrl();
    }

}