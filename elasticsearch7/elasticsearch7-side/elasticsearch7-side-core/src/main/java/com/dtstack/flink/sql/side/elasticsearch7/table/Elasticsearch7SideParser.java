/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.dtstack.flink.sql.side.elasticsearch7.table;

import com.dtstack.flink.sql.table.AbstractSideTableParser;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.util.ClassUtil;
import com.dtstack.flink.sql.util.MathUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * @description: elasticsearch side module core
 * @program: flink.sql
 * @author: lany
 * @create: 2021/01/11 10:21
 */
public class Elasticsearch7SideParser extends AbstractSideTableParser {

    private static final String KEY_ES7_ADDRESS = "address";

    private static final String KEY_ES7_CLUSTER = "cluster";

    private static final String KEY_ES7_INDEX = "index";

    private static final String KEY_ES7_AUTHMESH = "authMesh";

    private static final String KEY_ES7_USERNAME = "userName";

    private static final String KEY_ES7_PASSWORD = "password";

    private static final String KEY_TRUE = "true";

    @Override
    protected boolean fieldNameNeedsUpperCase() {
        return false;
    }

    @Override
    public AbstractTableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) throws Exception {

        Elasticsearch7SideTableInfo elasticsearch7SideTableInfo = new Elasticsearch7SideTableInfo();
        elasticsearch7SideTableInfo.setName(tableName);
        parseFieldsInfo(fieldsInfo, elasticsearch7SideTableInfo);
        parseCacheProp(elasticsearch7SideTableInfo, props);
        elasticsearch7SideTableInfo.setAddress((String) props.get(KEY_ES7_ADDRESS.toLowerCase()));
        elasticsearch7SideTableInfo.setClusterName((String) props.get(KEY_ES7_CLUSTER.toLowerCase()));
        elasticsearch7SideTableInfo.setIndex((String) props.get(KEY_ES7_INDEX.toLowerCase()));

        String authMethStr = (String) props.get(KEY_ES7_AUTHMESH.toLowerCase());
        if (authMethStr != null && StringUtils.equalsIgnoreCase(KEY_TRUE, authMethStr)) {
            elasticsearch7SideTableInfo.setAuthMesh(MathUtil.getBoolean(authMethStr));
            elasticsearch7SideTableInfo.setUserName(MathUtil.getString(props.get(KEY_ES7_USERNAME.toLowerCase())));
            elasticsearch7SideTableInfo.setPassword(MathUtil.getString(props.get(KEY_ES7_PASSWORD.toLowerCase())));
        }

        if (MathUtil.getLongVal(props.get(elasticsearch7SideTableInfo.ERROR_LIMIT.toLowerCase())) != null) {
            elasticsearch7SideTableInfo.setErrorLimit(MathUtil.getLongVal(props.get(elasticsearch7SideTableInfo.ERROR_LIMIT.toLowerCase())));
        }

        elasticsearch7SideTableInfo.check();
        return elasticsearch7SideTableInfo;
    }

    @Override
    public Class dbTypeConvertToJavaType(String fieldType) {
        return ClassUtil.stringConvertClass(fieldType);
    }
}
