package com.dtstack.flink.sql.sink.db.table;

import com.dtstack.flink.sql.core.rdb.JdbcCheckKeys;
import com.dtstack.flink.sql.sink.rdb.table.RdbSinkParser;
import com.dtstack.flink.sql.table.AbstractTableInfo;

import java.util.Map;

public class DbSinkParser extends RdbSinkParser {

    private static final String CURR_TYPE = "db2";

    @Override
    public AbstractTableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) {
        props.put(JdbcCheckKeys.DRIVER_NAME, "com.ibm.db2.jcc.DB2Driver");
        AbstractTableInfo tableInfo = super.getTableInfo(tableName, fieldsInfo, props);
        tableInfo.setType(CURR_TYPE);
        return tableInfo;
    }
}
