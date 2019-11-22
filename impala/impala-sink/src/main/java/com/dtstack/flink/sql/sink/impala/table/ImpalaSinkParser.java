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

import com.dtstack.flink.sql.sink.rdb.table.RdbSinkParser;
import com.dtstack.flink.sql.table.TableInfo;
import com.dtstack.flink.sql.util.MathUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Reason:
 * Date: 2019/11/11
 * Company: www.dtstack.com
 *
 * @author xiuzhu
 */

public class ImpalaSinkParser extends RdbSinkParser {

    private static final String CURR_TYPE = "impala";

    @Override
    public TableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) {
        ImpalaTableInfo impalaTableInfo = new ImpalaTableInfo();
        impalaTableInfo.setName(tableName);
        parseFieldsInfo(fieldsInfo, impalaTableInfo);

        impalaTableInfo.setParallelism(MathUtil.getIntegerVal(props.get(ImpalaTableInfo.PARALLELISM_KEY.toLowerCase())));
        impalaTableInfo.setUrl(MathUtil.getString(props.get(ImpalaTableInfo.URL_KEY.toLowerCase())));
        impalaTableInfo.setTableName(MathUtil.getString(props.get(ImpalaTableInfo.TABLE_NAME_KEY.toLowerCase())));
        impalaTableInfo.setBatchSize(MathUtil.getIntegerVal(props.get(ImpalaTableInfo.BATCH_SIZE_KEY.toLowerCase())));
        impalaTableInfo.setBatchWaitInterval(MathUtil.getLongVal(props.get(ImpalaTableInfo.BATCH_WAIT_INTERVAL_KEY.toLowerCase())));
        impalaTableInfo.setBufferSize(MathUtil.getString(props.get(ImpalaTableInfo.BUFFER_SIZE_KEY.toLowerCase())));
        impalaTableInfo.setFlushIntervalMs(MathUtil.getString(props.get(ImpalaTableInfo.FLUSH_INTERVALMS_KEY.toLowerCase())));
        impalaTableInfo.setSchema(MathUtil.getString(props.get(ImpalaTableInfo.SCHEMA_KEY.toLowerCase())));

        Integer authMech = MathUtil.getIntegerVal(props.get(ImpalaTableInfo.AUTHMECH_KEY.toLowerCase()));
        authMech = authMech == null? 0 : authMech;
        impalaTableInfo.setAuthMech(authMech);
        List authMechs = Arrays.asList(new Integer[]{0, 1, 2, 3});

        if (!authMechs.contains(authMech) ){
            throw new IllegalArgumentException("The value of authMech is illegal, Please select 0, 1, 2, 3");
        } else if (authMech == 1) {
            impalaTableInfo.setPrincipal(MathUtil.getString(props.get(ImpalaTableInfo.PRINCIPAL_KEY.toLowerCase())));
            impalaTableInfo.setKeyTabFilePath(MathUtil.getString(props.get(ImpalaTableInfo.KEYTABFILEPATH_KEY.toLowerCase())));
            impalaTableInfo.setKrb5FilePath(MathUtil.getString(props.get(ImpalaTableInfo.KRB5FILEPATH_KEY.toLowerCase())));
            String krbRealm = MathUtil.getString(props.get(ImpalaTableInfo.KRBREALM_KEY.toLowerCase()));
            krbRealm = krbRealm == null? "HADOOP.COM" : krbRealm;
            impalaTableInfo.setKrbRealm(krbRealm);
            impalaTableInfo.setKrbHostFQDN(MathUtil.getString(props.get(impalaTableInfo.KRBHOSTFQDN_KEY.toLowerCase())));
            impalaTableInfo.setKrbServiceName(MathUtil.getString(props.get(impalaTableInfo.KRBSERVICENAME_KEY.toLowerCase())));
        } else if (authMech == 2 ) {
            impalaTableInfo.setUserName(MathUtil.getString(props.get(ImpalaTableInfo.USER_NAME_KEY.toLowerCase())));
        } else if (authMech == 3) {
            impalaTableInfo.setUserName(MathUtil.getString(props.get(ImpalaTableInfo.USER_NAME_KEY.toLowerCase())));
            impalaTableInfo.setPassword(MathUtil.getString(props.get(ImpalaTableInfo.PASSWORD_KEY.toLowerCase())));
        }

        String enablePartitionStr  = (String) props.get(ImpalaTableInfo.ENABLEPARITION_KEY.toLowerCase());
        boolean enablePartition = MathUtil.getBoolean(enablePartitionStr == null? "false":enablePartitionStr);
        impalaTableInfo.setEnablePartition(enablePartition);
        if(enablePartition){
            String partitionFields = MathUtil.getString(props.get(ImpalaTableInfo.PARTITIONFIELDS_KEY.toLowerCase()));
            impalaTableInfo.setPartitionFields(partitionFields);
        }

        impalaTableInfo.check();
        return impalaTableInfo;
    }
}
