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

import com.dtstack.flink.sql.sink.impala.table.ImpalaTableInfo;
import com.dtstack.flink.sql.util.JDBCUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Reason:
 * Date: 2019/11/13
 *
 * @author xiuzhu
 */

public class ImpalaOutputFormat extends RetractJDBCOutputFormat {

    private ImpalaTableInfo impalaTableInfo;

    @Override
    public Connection establishConnection() throws SQLException, ClassNotFoundException, IOException {
        Connection connection ;
        String newUrl = "";
        String drivername = getDrivername();
        Integer authMech = impalaTableInfo.getAuthMech();

        JDBCUtils.forName(drivername, getClass().getClassLoader());

        StringBuffer urlBuffer = new StringBuffer(impalaTableInfo.getUrl());
        if (authMech == 0) {
            newUrl = urlBuffer.toString();
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
            configuration.set("hadoop.security.authentication" , "Kerberos");
            UserGroupInformation.setConfiguration(configuration);
            UserGroupInformation.loginUserFromKeytab(principal, keyTabFilePath);

        } else if (authMech == 2) {
            String userName = impalaTableInfo.getUserName();
            urlBuffer.append(";"
                .concat("AuthMech=3;")
                .concat("UID=").concat(userName).concat(";")
                .concat("PWD=;")
                .concat("UseSasl=0")
            );
            newUrl = urlBuffer.toString();
        } else if (authMech == 3) {
            String userName = impalaTableInfo.getUserName();
            String password = impalaTableInfo.getPassword();
            urlBuffer.append(";"
                .concat("AuthMech=3;")
                .concat("UID=").concat(userName).concat(";")
                .concat("PWD=").concat(password)
            );
            newUrl = urlBuffer.toString();
        } else {
            throw new IllegalArgumentException("The value of authMech is illegal, Please select 0, 1, 2, 3");
        }

        connection = DriverManager.getConnection(newUrl);
        connection.setAutoCommit(false);
        return connection;
    }

    @Override
    public void verifyField() {
        super.verifyField();
        if (impalaTableInfo == null) {
            throw new IllegalArgumentException("No impalaTableInfo supplied.");
        }

    }

    public ImpalaTableInfo getImpalaTableInfo() {
        return impalaTableInfo;
    }

    public void setImpalaTableInfo(ImpalaTableInfo impalaTableInfo) {
        this.impalaTableInfo = impalaTableInfo;
    }
}
