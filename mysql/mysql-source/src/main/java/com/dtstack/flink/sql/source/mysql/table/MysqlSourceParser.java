package com.dtstack.flink.sql.source.mysql.table;

import com.dtstack.flink.sql.table.AbstractSourceParser;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.util.MathUtil;
import org.apache.flink.api.java.io.jdbc.JDBCTableSourceSinkFactory;
import org.apache.flink.table.descriptors.JDBCValidator;

import java.util.Map;

public class MysqlSourceParser extends AbstractSourceParser {
    private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    public static final String URL_KEY = "url";
    public static final String USER_NAME_KEY = "userName";
    public static final String PASSWORD_KEY = "password";
    public static final String TABLE_NAME_KEY = "tableName";



    @Override
    public AbstractTableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) throws Exception {
        MysqlSourceTableInfo mysqlSourceTableInfo = new MysqlSourceTableInfo();
        parseFieldsInfo(fieldsInfo, mysqlSourceTableInfo);
        mysqlSourceTableInfo.setName(tableName);
        mysqlSourceTableInfo.setType(MathUtil.getString(props.get("type")));

        JDBCTableSourceSinkFactory jdbcTableSourceSinkFactory = new JDBCTableSourceSinkFactory();
        Map<String, String> properties = jdbcTableSourceSinkFactory.requiredContext();
        properties.put(JDBCValidator.CONNECTOR_DRIVER, MYSQL_DRIVER);
        props.forEach((key, value) -> {
            if(URL_KEY.equalsIgnoreCase(key)) {
                properties.put(JDBCValidator.CONNECTOR_URL, String.valueOf(value));
            } else if(USER_NAME_KEY.equalsIgnoreCase(key)) {
                properties.put(JDBCValidator.CONNECTOR_USERNAME, String.valueOf(value));
            } else if(PASSWORD_KEY.equalsIgnoreCase(key)) {
                properties.put(JDBCValidator.CONNECTOR_PASSWORD, String.valueOf(value));
            } else if(TABLE_NAME_KEY.equalsIgnoreCase(key)) {
                properties.put(JDBCValidator.CONNECTOR_TABLE, String.valueOf(value));
            } else {
                properties.put(key, String.valueOf(value));
            }
        });
        mysqlSourceTableInfo.setProps(properties);
        return mysqlSourceTableInfo;
    }
}
