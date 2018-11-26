package com.dtstack.flink.sql.side.sqlserver.table;

import com.dtstack.flink.sql.side.rdb.table.RdbSideParser;
import com.dtstack.flink.sql.table.TableInfo;
import java.util.Map;


public class SqlserverSideParser extends RdbSideParser {
    private static final String CURR_TYPE = "sqlserver";

    @Override
    public TableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) {
        TableInfo sqlServerTableInfo = super.getTableInfo(tableName, fieldsInfo, props);
        sqlServerTableInfo.setType(CURR_TYPE);
        return sqlServerTableInfo;
    }
}
