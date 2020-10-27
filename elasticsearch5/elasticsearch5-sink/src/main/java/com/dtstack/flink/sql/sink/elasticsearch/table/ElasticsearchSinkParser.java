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

 

package com.dtstack.flink.sql.sink.elasticsearch.table;


import com.dtstack.flink.sql.table.AbstractTableParser;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.util.MathUtil;

import java.util.Map;

/**
 * @date 2018/09/12
 * @author sishu.yss
 * @Company: www.dtstack.com
 */
public class ElasticsearchSinkParser extends AbstractTableParser {

    private static final String KEY_ES_ADDRESS = "address";

    private static final String KEY_ES_CLUSTER = "cluster";

    private static final String KEY_ES_INDEX = "index";

    private static final String KEY_ES_TYPE = "estype";

    private static final String KEY_ES_ID_FIELD_INDEX_LIST = "id";

    private static final String KEY_ES_AUTHMESH = "authMesh";

    private static final String KEY_ES_USERNAME = "userName";

    private static final String KEY_ES_PASSWORD = "password";

    private static final String KEY_ES_PARALLELISM = "parallelism";

    @Override
    protected boolean fieldNameNeedsUpperCase() {
        return false;
    }

    @Override
    public AbstractTableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) {
        ElasticsearchTableInfo elasticsearchTableInfo = new ElasticsearchTableInfo();
        elasticsearchTableInfo.setName(tableName);
        parseFieldsInfo(fieldsInfo, elasticsearchTableInfo);
        elasticsearchTableInfo.setAddress((String) props.get(KEY_ES_ADDRESS.toLowerCase()));
        elasticsearchTableInfo.setClusterName((String) props.get(KEY_ES_CLUSTER.toLowerCase()));
        elasticsearchTableInfo.setId((String) props.get(KEY_ES_ID_FIELD_INDEX_LIST.toLowerCase()));
        elasticsearchTableInfo.setIndex((String) props.get(KEY_ES_INDEX.toLowerCase()));
        elasticsearchTableInfo.setEsType((String) props.get(KEY_ES_TYPE.toLowerCase()));
        elasticsearchTableInfo.setParallelism(MathUtil.getIntegerVal(props.getOrDefault(KEY_ES_PARALLELISM.toLowerCase(), 1)));

        String authMeshStr = (String)props.get(KEY_ES_AUTHMESH.toLowerCase());
        if (authMeshStr != null & "true".equals(authMeshStr)) {
            elasticsearchTableInfo.setAuthMesh(MathUtil.getBoolean(authMeshStr));
            elasticsearchTableInfo.setUserName(MathUtil.getString(props.get(KEY_ES_USERNAME.toLowerCase())));
            elasticsearchTableInfo.setPassword(MathUtil.getString(props.get(KEY_ES_PASSWORD.toLowerCase())));
        }
        elasticsearchTableInfo.check();
        return elasticsearchTableInfo;
    }
}
