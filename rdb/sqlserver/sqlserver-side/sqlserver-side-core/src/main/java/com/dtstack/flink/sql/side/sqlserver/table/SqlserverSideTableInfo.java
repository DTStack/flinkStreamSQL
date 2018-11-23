package com.dtstack.flink.sql.side.sqlserver.table;

import com.dtstack.flink.sql.side.SideTableInfo;


public class SqlserverSideTableInfo extends SideTableInfo {

    private static final long serialVersionUID = -1L;

    private static final String CURR_TYPE = "sqlserver";

    public static final String URL_KEY = "url";

    public static final String TABLE_NAME_KEY = "tableName";

    public static final String USER_NAME_KEY = "userName";

    public static final String PASSWORD_KEY = "password";

    public SqlserverSideTableInfo() {
        setType(CURR_TYPE);
    }

    private String url;

    private String tableName;

    private String userName;

    private String password;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    @Override
    public boolean check() {
        return false;
    }

}
