package com.dtstack.flink.sql.side.elasticsearch6.table;

import com.dtstack.flink.sql.table.AbsSideTableParser;
import com.dtstack.flink.sql.table.TableInfo;
import com.dtstack.flink.sql.util.MathUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * @author yinxi
 * @date 2020/1/13 - 1:07
 */
public class Elasticsearch6SideParser extends AbsSideTableParser {

    private static final String KEY_ES6_ADDRESS = "address";

    private static final String KEY_ES6_CLUSTER = "cluster";

    private static final String KEY_ES6_INDEX = "index";

    private static final String KEY_ES6_TYPE = "estype";

    private static final String KEY_ES6_AUTHMESH = "authMesh";

    private static final String KEY_ES6_USERNAME = "userName";

    private static final String KEY_ES6_PASSWORD = "password";

    @Override
    protected boolean fieldNameNeedsUpperCase() {
        return false;
    }

    @Override
    public TableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) {
        Elasticsearch6SideTableInfo elasticsearch6SideTableInfo = new Elasticsearch6SideTableInfo();
        elasticsearch6SideTableInfo.setName(tableName);
        parseFieldsInfo(fieldsInfo, elasticsearch6SideTableInfo);
        elasticsearch6SideTableInfo.setAddress((String) props.get(KEY_ES6_ADDRESS.toLowerCase()));
        elasticsearch6SideTableInfo.setClusterName((String) props.get(KEY_ES6_CLUSTER.toLowerCase()));
        elasticsearch6SideTableInfo.setIndex((String) props.get(KEY_ES6_INDEX.toLowerCase()));
        elasticsearch6SideTableInfo.setEsType((String) props.get(KEY_ES6_TYPE.toLowerCase()));

        String authMeshStr = (String) props.get(KEY_ES6_AUTHMESH.toLowerCase());
        if (authMeshStr != null && StringUtils.equalsIgnoreCase("true", authMeshStr)) {
            elasticsearch6SideTableInfo.setAuthMesh(MathUtil.getBoolean(authMeshStr));
            elasticsearch6SideTableInfo.setUserName(MathUtil.getString(props.get(KEY_ES6_USERNAME.toLowerCase())));
            elasticsearch6SideTableInfo.setPassword(MathUtil.getString(props.get(KEY_ES6_PASSWORD.toLowerCase())));
        }
        elasticsearch6SideTableInfo.check();
        return elasticsearch6SideTableInfo;
    }
}
