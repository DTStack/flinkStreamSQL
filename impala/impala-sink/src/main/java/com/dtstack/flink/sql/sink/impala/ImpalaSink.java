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

package com.dtstack.flink.sql.sink.impala;

import com.dtstack.flink.sql.sink.IStreamSinkGener;
import com.dtstack.flink.sql.sink.impala.table.ImpalaTableInfo;
import com.dtstack.flink.sql.sink.rdb.JDBCOptions;
import com.dtstack.flink.sql.sink.rdb.RdbSink;
import com.dtstack.flink.sql.sink.rdb.dialect.JDBCDialect;
import com.dtstack.flink.sql.sink.rdb.format.JDBCUpsertOutputFormat;
import com.dtstack.flink.sql.table.TargetTableInfo;
import com.dtstack.flink.sql.util.JDBCUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Date: 2019/11/11
 * Company: www.dtstack.com
 *
 * @author xiuzhu
 */

public class ImpalaSink extends RdbSink implements IStreamSinkGener<RdbSink> {

    private ImpalaTableInfo impalaTableInfo;

    public ImpalaSink() {
        super(new ImpalaDialect());
    }

    @Override
    public JDBCUpsertOutputFormat getOutputFormat() {
        JDBCOptions jdbcOptions = JDBCOptions.builder()
                .setDBUrl(getImpalaJdbcUrl()).setDialect(jdbcDialect)
                .setUsername(userName).setPassword(password)
                .setTableName(tableName).build();

        return JDBCUpsertOutputFormat.builder()
                .setOptions(jdbcOptions)
                .setFieldNames(fieldNames)
                .setFlushMaxSize(batchNum)
                .setFlushIntervalMills(batchWaitInterval)
                .setFieldTypes(sqlTypes)
                .setKeyFields(primaryKeys)
                .setPartitionFields(impalaTableInfo.getPartitionFields())
                .setAllReplace(allReplace)
                .setUpdateMode(updateMode).build();
    }


    public String getImpalaJdbcUrl() {
        Integer authMech = impalaTableInfo.getAuthMech();
        String newUrl = dbURL;
        StringBuffer urlBuffer = new StringBuffer(dbURL);
        if (authMech == 0) {
            return newUrl;
        } else if (authMech == 1) {
            String keyTabFilePath = impalaTableInfo.getKeyTabFilePath();
            String krb5FilePath = impalaTableInfo.getKrb5FilePath();
            String principal = impalaTableInfo.getPrincipal();
            String krbRealm = impalaTableInfo.getKrbRealm();
            String krbHostFQDN = impalaTableInfo.getKrbHostFQDN();
            String krbServiceName = impalaTableInfo.getKrbServiceName();
            urlBuffer.append(";"
                    .concat("AuthMech=1;")
                    .concat("KrbRealm=").concat(krbRealm).concat(";")
                    .concat("KrbHostFQDN=").concat(krbHostFQDN).concat(";")
                    .concat("KrbServiceName=").concat(krbServiceName).concat(";")
            );
            newUrl = urlBuffer.toString();

            System.setProperty("java.security.krb5.conf", krb5FilePath);
            Configuration configuration = new Configuration();
            configuration.set("hadoop.security.authentication", "Kerberos");
            UserGroupInformation.setConfiguration(configuration);
            try {
                UserGroupInformation.loginUserFromKeytab(principal, keyTabFilePath);
            } catch (IOException e) {
                throw new RuntimeException("loginUserFromKeytab error ..", e);
            }

        } else if (authMech == 2) {
            urlBuffer.append(";"
                    .concat("AuthMech=3;")
                    .concat("UID=").concat(userName).concat(";")
                    .concat("PWD=;")
                    .concat("UseSasl=0")
            );
            newUrl = urlBuffer.toString();
        } else if (authMech == 3) {
            urlBuffer.append(";"
                    .concat("AuthMech=3;")
                    .concat("UID=").concat(userName).concat(";")
                    .concat("PWD=").concat(password)
            );
            newUrl = urlBuffer.toString();
        } else {
            throw new IllegalArgumentException("The value of authMech is illegal, Please select 0, 1, 2, 3");
        }
        return newUrl;
    }

    @Override
    public RdbSink genStreamSink(TargetTableInfo targetTableInfo) {
        super.genStreamSink(targetTableInfo);
        this.impalaTableInfo = (ImpalaTableInfo) targetTableInfo;
        return this;
    }

}
