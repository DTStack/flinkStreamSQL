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

import com.dtstack.flink.sql.sink.rdb.table.RdbTableInfo;
import com.google.common.base.Preconditions;

/**
 * Date: 2019/11/13
 * Company: www.dtstack.com
 *
 * @author xiuzhu
 */

public class ImpalaTableInfo extends RdbTableInfo {

    public static final String AUTHMECH_KEY = "authMech";

    public static final String KRB5FILEPATH_KEY = "krb5FilePath";

    public static final String PRINCIPAL_KEY = "principal";

    public static final String KEYTABFILEPATH_KEY = "keyTabFilePath";

    public static final String KRBREALM_KEY = "krbRealm";

    public static final String KRBHOSTFQDN_KEY = "krbHostFQDN";

    public static final String KRBSERVICENAME_KEY = "krbServiceName";

    private static final String CURR_TYPE = "impala";

    private Integer authMech;

    private String krb5FilePath;

    private String principal;

    private String keyTabFilePath;

    private String krbRealm;

    private String krbHostFQDN;

    private String krbServiceName;

    public ImpalaTableInfo() {
        setType(CURR_TYPE);
    }

    public Integer getAuthMech() {
        return authMech;
    }

    public void setAuthMech(Integer authMech) {
        this.authMech = authMech;
    }

    public String getKrb5FilePath() {
        return krb5FilePath;
    }

    public void setKrb5FilePath(String krb5FilePath) {
        this.krb5FilePath = krb5FilePath;
    }

    public String getPrincipal() {
        return principal;
    }

    public void setPrincipal(String principal) {
        this.principal = principal;
    }

    public String getKeyTabFilePath() {
        return keyTabFilePath;
    }

    public void setKeyTabFilePath(String keyTabFilePath) {
        this.keyTabFilePath = keyTabFilePath;
    }

    public String getKrbRealm() {
        return krbRealm;
    }

    public void setKrbRealm(String krbRealm) {
        this.krbRealm = krbRealm;
    }

    public String getKrbHostFQDN() {
        return krbHostFQDN;
    }

    public void setKrbHostFQDN(String krbHostFQDN) {
        this.krbHostFQDN = krbHostFQDN;
    }

    public String getKrbServiceName() {
        return krbServiceName;
    }

    public void setKrbServiceName(String krbServiceName) {
        this.krbServiceName = krbServiceName;
    }

    @Override
    public boolean check() {
        Preconditions.checkNotNull(this.getUrl(), "impala field of url is required");
        Preconditions.checkNotNull(this.getTableName(), "impala field of tableName is required");
        Preconditions.checkNotNull(this.getAuthMech(), "impala field of authMech is required");
        Integer authMech = getAuthMech();

        if (authMech == 1) {
            Preconditions.checkNotNull(this.getKrb5FilePath(), "impala field of krb5FilePath is required");
            Preconditions.checkNotNull(this.getPrincipal(), "impala field of principal is required");
            Preconditions.checkNotNull(this.getKeyTabFilePath(), "impala field of keyTabFilePath is required");
            Preconditions.checkNotNull(this.getKrbHostFQDN(), "impala field of krbHostFQDN is required");
            Preconditions.checkNotNull(this.getKrbServiceName(), "impala field of krbServiceName is required");
        } else if (authMech == 2) {
            Preconditions.checkNotNull(this.getUserName(), "impala field of userName is required");
        }else if (authMech == 3) {
            Preconditions.checkNotNull(this.getUserName(), "impala field of userName is required");
            Preconditions.checkNotNull(this.getPassword(), "impala field of password is required");
        }
        return true;
    }
}
