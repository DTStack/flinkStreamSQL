package com.dtstack.flink.sql.side.polardb.table;

import com.dtstack.flink.sql.side.rdb.table.RdbSideParser;
import com.dtstack.flink.sql.table.TableInfo;

import java.util.Map;

public class PolardbSideParser extends RdbSideParser {
    private static final String CURR_TYPE = "polardb";

    @Override
    public TableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) {
        TableInfo mysqlTableInfo = super.getTableInfo(tableName, fieldsInfo, props);
        mysqlTableInfo.setType(CURR_TYPE);
        return mysqlTableInfo;
    }
}
