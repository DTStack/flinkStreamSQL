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

package com.dtstack.flink.sql.side.elasticsearch6.table;

import com.dtstack.flink.sql.side.elasticsearch6.util.ClassUtil;
import com.dtstack.flink.sql.table.AbstractSideTableParser;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.util.MathUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;

/**
 * @author yinxi
 * @date 2020/1/13 - 1:07
 */
public class Elasticsearch6SideParser extends AbstractSideTableParser {

    private static final String KEY_ES6_ADDRESS = "address";

    private static final String KEY_ES6_CLUSTER = "cluster";

    private static final String KEY_ES6_INDEX = "index";

    private static final String KEY_ES6_TYPE = "esType";

    private static final String KEY_ES6_AUTHMESH = "authMesh";

    private static final String KEY_ES6_USERNAME = "userName";

    private static final String KEY_ES6_PASSWORD = "password";

    private static final String KEY_TRUE = "true";


    @Override
    protected boolean fieldNameNeedsUpperCase() {
        return false;
    }

    @Override
    public AbstractTableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) {
        Elasticsearch6SideTableInfo elasticsearch6SideTableInfo = new Elasticsearch6SideTableInfo();
        elasticsearch6SideTableInfo.setName(tableName);
        parseFieldsInfo(fieldsInfo, elasticsearch6SideTableInfo);
        parseCacheProp(elasticsearch6SideTableInfo, props);
        elasticsearch6SideTableInfo.setAddress((String) props.get(KEY_ES6_ADDRESS.toLowerCase()));
        elasticsearch6SideTableInfo.setClusterName((String) props.get(KEY_ES6_CLUSTER.toLowerCase()));
        elasticsearch6SideTableInfo.setIndex((String) props.get(KEY_ES6_INDEX.toLowerCase()));
        elasticsearch6SideTableInfo.setEsType((String) props.get(KEY_ES6_TYPE.toLowerCase()));

        String authMeshStr = (String) props.get(KEY_ES6_AUTHMESH.toLowerCase());
        if (authMeshStr != null && StringUtils.equalsIgnoreCase(KEY_TRUE, authMeshStr)) {
            elasticsearch6SideTableInfo.setAuthMesh(MathUtil.getBoolean(authMeshStr));
            elasticsearch6SideTableInfo.setUserName(MathUtil.getString(props.get(KEY_ES6_USERNAME.toLowerCase())));
            elasticsearch6SideTableInfo.setPassword(MathUtil.getString(props.get(KEY_ES6_PASSWORD.toLowerCase())));
        }
        elasticsearch6SideTableInfo.check();
        return elasticsearch6SideTableInfo;
    }

    @Override
    public Class dbTypeConvertToJavaType(String fieldType) {
        return ClassUtil.stringConvertClass(fieldType);
    }
}
