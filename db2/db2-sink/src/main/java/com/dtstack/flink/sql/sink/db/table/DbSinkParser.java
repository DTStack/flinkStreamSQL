package com.dtstack.flink.sql.sink.db.table;

import com.dtstack.flink.sql.sink.rdb.table.RdbSinkParser;
import com.dtstack.flink.sql.table.TableInfo;

import java.util.Map;

public class DbSinkParser extends RdbSinkParser {

    private static final String CURR_TYPE = "db2";

    @Override
    public TableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) {
        TableInfo tableInfo = super.getTableInfo(tableName, fieldsInfo, props);
        tableInfo.setType(CURR_TYPE);
        return tableInfo;
    }
}
