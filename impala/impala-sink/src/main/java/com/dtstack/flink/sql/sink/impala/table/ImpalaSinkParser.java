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

package com.dtstack.flink.sql.sink.impala.table;

import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.table.AbstractTableParser;
import com.dtstack.flink.sql.util.MathUtil;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Date: 2020/10/14
 * Company: www.dtstack.com
 *
 * @author tiezhu
 */
public class ImpalaSinkParser extends AbstractTableParser {

    private static final String PARALLELISM_KEY = "parallelism";

    private static final String AUTH_MECH_KEY = "authMech";

    private static final String KRB5FILEPATH_KEY = "krb5FilePath";

    private static final String PRINCIPAL_KEY = "principal";

    private static final String KEY_TAB_FILE_PATH_KEY = "keyTabFilePath";

    private static final String KRB_REALM_KEY = "krbRealm";

    private static final String KRB_HOST_FQDN_KEY = "krbHostFQDN";

    private static final String KRB_SERVICE_NAME_KEY = "krbServiceName";

    private static final String ENABLE_PARTITION_KEY = "enablePartition";

    private static final String PARTITION_FIELDS_KEY = "partitionFields";

    private static final String URL_KEY = "url";

    private static final String TABLE_NAME_KEY = "tableName";

    private static final String USER_NAME_KEY = "userName";

    private static final String PASSWORD_KEY = "password";

    private static final String BATCH_SIZE_KEY = "batchSize";

    private static final String BATCH_WAIT_INTERVAL_KEY = "batchWaitInterval";

    private static final String BUFFER_SIZE_KEY = "bufferSize";

    private static final String FLUSH_INTERVAL_MS_KEY = "flushIntervalMs";

    private static final String SCHEMA_KEY = "schema";

    private static final String UPDATE_KEY = "updateMode";

    private static final String KUDU_TYPE = "kudu";

    private static final String STORE_TYPE_KEY = "storeType";

    private static final String KRB_DEFAULT_REALM = "HADOOP.COM";

    private static final String CURRENT_TYPE = "impala";


    @Override
    public AbstractTableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) throws Exception {
        ImpalaTableInfo impalaTableInfo = new ImpalaTableInfo();
        impalaTableInfo.setName(tableName);
        parseFieldsInfo(fieldsInfo, impalaTableInfo);

        impalaTableInfo.setParallelism(MathUtil.getIntegerVal(props.get(PARALLELISM_KEY.toLowerCase())));
        impalaTableInfo.setUrl(MathUtil.getString(props.get(URL_KEY.toLowerCase())));
        impalaTableInfo.setTableName(MathUtil.getString(props.get(TABLE_NAME_KEY.toLowerCase())));
        impalaTableInfo.setBatchSize(MathUtil.getIntegerVal(props.get(BATCH_SIZE_KEY.toLowerCase())));
        impalaTableInfo.setBatchWaitInterval(MathUtil.getLongVal(props.get(BATCH_WAIT_INTERVAL_KEY.toLowerCase())));
        impalaTableInfo.setBufferSize(MathUtil.getString(props.get(BUFFER_SIZE_KEY.toLowerCase())));
        impalaTableInfo.setFlushIntervalMs(MathUtil.getString(props.get(FLUSH_INTERVAL_MS_KEY.toLowerCase())));
        impalaTableInfo.setSchema(MathUtil.getString(props.get(SCHEMA_KEY.toLowerCase())));
        impalaTableInfo.setUpdateMode(MathUtil.getString(props.get(UPDATE_KEY.toLowerCase())));

        Integer authMech = MathUtil.getIntegerVal(props.get(AUTH_MECH_KEY.toLowerCase()));
        authMech = authMech == null ? 0 : authMech;
        impalaTableInfo.setAuthMech(authMech);
        List<Integer> authMechs = Arrays.asList(0, 1, 2, 3);

        if (!authMechs.contains(authMech)) {
            throw new IllegalArgumentException("The value of authMech is illegal, Please select 0, 1, 2, 3");
        } else if (authMech == 1) {
            impalaTableInfo.setPrincipal(MathUtil.getString(props.get(PRINCIPAL_KEY.toLowerCase())));
            impalaTableInfo.setKeyTabFilePath(MathUtil.getString(props.get(KEY_TAB_FILE_PATH_KEY.toLowerCase())));
            impalaTableInfo.setKrb5FilePath(MathUtil.getString(props.get(KRB5FILEPATH_KEY.toLowerCase())));
            String krbRealm = MathUtil.getString(props.get(KRB_REALM_KEY.toLowerCase()));
            krbRealm = krbRealm == null ? KRB_DEFAULT_REALM : krbRealm;
            impalaTableInfo.setKrbRealm(krbRealm);
            impalaTableInfo.setKrbHostFQDN(MathUtil.getString(props.get(KRB_HOST_FQDN_KEY.toLowerCase())));
            impalaTableInfo.setKrbServiceName(MathUtil.getString(props.get(KRB_SERVICE_NAME_KEY.toLowerCase())));
        } else if (authMech == 2) {
            impalaTableInfo.setUserName(MathUtil.getString(props.get(USER_NAME_KEY.toLowerCase())));
        } else if (authMech == 3) {
            impalaTableInfo.setUserName(MathUtil.getString(props.get(USER_NAME_KEY.toLowerCase())));
            impalaTableInfo.setPassword(MathUtil.getString(props.get(PASSWORD_KEY.toLowerCase())));
        }

        String storeType = MathUtil.getString(props.get(STORE_TYPE_KEY.toLowerCase()));
        impalaTableInfo.setStoreType(storeType);

        String enablePartitionStr = (String) props.get(ENABLE_PARTITION_KEY.toLowerCase());
        boolean enablePartition = MathUtil.getBoolean(enablePartitionStr == null ? "false" : enablePartitionStr);
        impalaTableInfo.setEnablePartition(enablePartition);

        if (!storeType.equalsIgnoreCase(KUDU_TYPE) && enablePartition) {
            String partitionFields = MathUtil.getString(props.get(PARTITION_FIELDS_KEY.toLowerCase()));
            impalaTableInfo.setPartitionFields(partitionFields);
        }

        impalaTableInfo.setType(CURRENT_TYPE);

        impalaTableInfo.check();
        return impalaTableInfo;
    }
}
