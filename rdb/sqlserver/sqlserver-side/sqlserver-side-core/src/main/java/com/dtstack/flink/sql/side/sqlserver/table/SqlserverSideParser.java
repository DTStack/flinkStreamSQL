package com.dtstack.flink.sql.side.sqlserver.table;

import com.dtstack.flink.sql.table.AbsSideTableParser;
import com.dtstack.flink.sql.table.TableInfo;
import com.dtstack.flink.sql.util.MathUtil;

import java.util.Map;

public class SqlserverSideParser extends AbsSideTableParser {


    @Override
    public TableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) {
        SqlserverSideTableInfo sqlserverSideTableInfo = new SqlserverSideTableInfo();
        sqlserverSideTableInfo.setName(tableName);

        parseFieldsInfo(fieldsInfo, sqlserverSideTableInfo);
        parseCacheProp(sqlserverSideTableInfo, props);

        sqlserverSideTableInfo.setParallelism(MathUtil.getIntegerVal(props.get(SqlserverSideTableInfo.PARALLELISM_KEY.toLowerCase())));
        sqlserverSideTableInfo.setUrl(MathUtil.getString(props.get(SqlserverSideTableInfo.URL_KEY.toLowerCase())));
        sqlserverSideTableInfo.setTableName(MathUtil.getString(props.get(SqlserverSideTableInfo.TABLE_NAME_KEY.toLowerCase())));
        sqlserverSideTableInfo.setUserName(MathUtil.getString(props.get(SqlserverSideTableInfo.USER_NAME_KEY.toLowerCase())));
        sqlserverSideTableInfo.setPassword(MathUtil.getString(props.get(SqlserverSideTableInfo.PASSWORD_KEY.toLowerCase())));

        return sqlserverSideTableInfo;
    }
}
