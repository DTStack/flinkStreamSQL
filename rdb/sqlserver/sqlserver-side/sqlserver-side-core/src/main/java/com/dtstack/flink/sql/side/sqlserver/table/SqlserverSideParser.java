package com.dtstack.flink.sql.side.sqlserver.table;

import com.dtstack.flink.sql.table.AbsSideTableParser;
import com.dtstack.flink.sql.table.TableInfo;
import com.dtstack.flink.sql.util.MathUtil;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqlserverSideParser extends AbsSideTableParser {
    private final static String SIDE_SIGN_KEY = "sideSignKey";

    private final static Pattern SIDE_TABLE_SIGN = Pattern.compile("(?i)^PERIOD\\s+FOR\\s+SYSTEM_TIME$");

    static {
        keyPatternMap.put(SIDE_SIGN_KEY, SIDE_TABLE_SIGN);
        keyHandlerMap.put(SIDE_SIGN_KEY, SqlserverSideParser::dealSideSign);
    }


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

    private static void dealSideSign(Matcher matcher, TableInfo tableInfo) {
    }
}
