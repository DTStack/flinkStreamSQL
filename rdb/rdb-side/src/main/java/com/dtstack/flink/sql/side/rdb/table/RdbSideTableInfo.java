package com.dtstack.flink.sql.side.rdb.table;

import com.dtstack.flink.sql.side.SideTableInfo;
import org.apache.flink.calcite.shaded.com.google.common.base.Preconditions;

/**
 * Reason:
 * Date: 2018/11/26
 * Company: www.dtstack.com
 *
 * @author maqi
 */
public class RdbSideTableInfo extends SideTableInfo {
    private static final long serialVersionUID = -1L;

    public static final String URL_KEY = "url";

    public static final String TABLE_NAME_KEY = "tableName";

    public static final String USER_NAME_KEY = "userName";

    public static final String PASSWORD_KEY = "password";

    @Override
    public boolean check() {
        Preconditions.checkNotNull(url, "rdb of URL is required");
        Preconditions.checkNotNull(tableName, "rdb of tableName is required");
        Preconditions.checkNotNull(userName, "rdb of userName is required");
        Preconditions.checkNotNull(password, "rdb of password is required");
        return true;
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
}
