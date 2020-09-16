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

package com.dtstack.flink.sql.side.impala;

import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.impala.table.ImpalaSideTableInfo;
import com.dtstack.flink.sql.side.rdb.all.AbstractRdbAllReqRow;
import com.dtstack.flink.sql.util.JDBCUtils;
import com.dtstack.flink.sql.util.KrbUtils;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

/**
 * side operator with cache for all(period reload)
 * Date: 2019/11/12
 * Company: www.dtstack.com
 *
 * @author xiuzhu
 */

public class ImpalaAllReqRow extends AbstractRdbAllReqRow {

    private static final long serialVersionUID = 2098635140857937717L;

    private static final Logger LOG = LoggerFactory.getLogger(ImpalaAllReqRow.class);

    private static final String IMPALA_DRIVER = "com.cloudera.impala.jdbc41.Driver";

    private ImpalaSideTableInfo impalaSideTableInfo;

    public ImpalaAllReqRow(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, AbstractSideTableInfo sideTableInfo) {
        super(new ImpalaAllSideInfo(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo));
        this.impalaSideTableInfo = (ImpalaSideTableInfo) sideTableInfo;
    }

    @Override
    public Connection getConn(String dbUrl, String userName, String password) {
        try {
            Connection connection;
            String url = getUrl();
            JDBCUtils.forName(IMPALA_DRIVER, getClass().getClassLoader());
            // Kerberos
            if (impalaSideTableInfo.getAuthMech() == 1) {
                String keyTabFilePath = impalaSideTableInfo.getKeyTabFilePath();
                String krb5FilePath = impalaSideTableInfo.getKrb5FilePath();
                String principal = impalaSideTableInfo.getPrincipal();
                UserGroupInformation ugi = KrbUtils.getUgi(principal, keyTabFilePath, krb5FilePath);
                connection = ugi.doAs(new PrivilegedExceptionAction<Connection>() {
                    @Override
                    public Connection run() throws SQLException {
                        return DriverManager.getConnection(url);
                    }
                });
            } else {
                connection = DriverManager.getConnection(url);
            }
            connection.setAutoCommit(false);
            return connection;
        } catch (Exception e) {
            LOG.error("", e);
            throw new RuntimeException("", e);
        }
    }

    public String getUrl() throws IOException {

        String newUrl = "";
        Integer authMech = impalaSideTableInfo.getAuthMech();

        StringBuffer urlBuffer = new StringBuffer(impalaSideTableInfo.getUrl());
        if (authMech == 0) {
            newUrl = urlBuffer.toString();

        } else if (authMech == 1) {
            String krbRealm = impalaSideTableInfo.getKrbRealm();
            String krbHostFQDN = impalaSideTableInfo.getKrbHostFQDN();
            String krbServiceName = impalaSideTableInfo.getKrbServiceName();
            urlBuffer.append(";"
                    .concat("AuthMech=1;")
                    .concat("KrbRealm=").concat(krbRealm).concat(";")
                    .concat("KrbHostFQDN=").concat(krbHostFQDN).concat(";")
                    .concat("KrbServiceName=").concat(krbServiceName).concat(";")
            );
            newUrl = urlBuffer.toString();


        } else if (authMech == 2) {
            String uName = impalaSideTableInfo.getUserName();
            urlBuffer.append(";"
                    .concat("AuthMech=3;")
                    .concat("UID=").concat(uName).concat(";")
                    .concat("PWD=;")
                    .concat("UseSasl=0")
            );
            newUrl = urlBuffer.toString();

        } else if (authMech == 3) {
            String uName = impalaSideTableInfo.getUserName();
            String pwd = impalaSideTableInfo.getPassword();
            urlBuffer.append(";"
                    .concat("AuthMech=3;")
                    .concat("UID=").concat(uName).concat(";")
                    .concat("PWD=").concat(pwd)
            );
            newUrl = urlBuffer.toString();

        } else {
            throw new IllegalArgumentException("The value of authMech is illegal, Please select 0, 1, 2, 3");
        }

        return newUrl;
    }
}
