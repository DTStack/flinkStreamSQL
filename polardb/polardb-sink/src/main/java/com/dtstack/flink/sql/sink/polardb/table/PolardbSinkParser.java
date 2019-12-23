package com.dtstack.flink.sql.sink.polardb.table;

import com.dtstack.flink.sql.sink.rdb.table.RdbSinkParser;
import com.dtstack.flink.sql.table.TableInfo;

import java.util.Map;

public class PolardbSinkParser extends RdbSinkParser {
    private static final String CURR_TYPE = "polardb";

    @Override
    public TableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) {
        TableInfo mysqlTableInfo = super.getTableInfo(tableName, fieldsInfo, props);
        mysqlTableInfo.setType(CURR_TYPE);
        return mysqlTableInfo;
    }
}
