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


import com.dtstack.flink.sql.krb.KerberosTable;
import com.dtstack.flink.sql.table.AbstractTargetTableInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.util.Arrays;

/**
 * @date 2018/09/12
 * @author sishu.yss
 * @Company: www.dtstack.com
 */
public class ElasticsearchTableInfo extends AbstractTargetTableInfo implements KerberosTable {

    private static final String CURR_TYPE = "elasticsearch-xh";

    private String address;

    private String index;

    private String id;

    private String clusterName;

    private String esType;


    /**
     * kerberos
     */
    private String principal;
    private String keytab;
    private String krb5conf;
    boolean enableKrb;

    @Override
    public String getPrincipal() {
        return principal;
    }

    @Override
    public void setPrincipal(String principal) {
        this.principal = principal;
    }

    @Override
    public String getKeytab() {
        return keytab;
    }

    @Override
    public void setKeytab(String keytab) {
        this.keytab = keytab;
    }

    @Override
    public String getKrb5conf() {
        return krb5conf;
    }

    @Override
    public void setKrb5conf(String krb5conf) {
        this.krb5conf = krb5conf;
    }

    @Override
    public boolean isEnableKrb() {
        return enableKrb;
    }

    @Override
    public void setEnableKrb(boolean enableKrb) {
        this.enableKrb = enableKrb;
    }

    public String getEsType() {
        return esType;
    }

    public void setEsType(String esType) {
        this.esType = esType;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    @Override
    public String getType() {
        //return super.getType().toLowerCase() + TARGET_SUFFIX;
        return super.getType().toLowerCase();
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public ElasticsearchTableInfo() {
        setType(CURR_TYPE);
    }

    @Override
    public boolean check() {
        Preconditions.checkNotNull(address, "elasticsearch type of address is required");
        Preconditions.checkNotNull(index, "elasticsearch type of index is required");
        Preconditions.checkNotNull(esType, "elasticsearch type of type is required");
        Preconditions.checkNotNull(clusterName, "elasticsearch type of clusterName is required");

        if (!StringUtils.isEmpty(id)) {
            Arrays.stream(StringUtils.split(id, ",")).forEach(number -> {
                Preconditions.checkArgument(NumberUtils.isNumber(number), "id must be a numeric type");
            });
        }

        boolean allNotSet =
                Strings.isNullOrEmpty(principal) &&
                        Strings.isNullOrEmpty(keytab) &&
                        Strings.isNullOrEmpty(krb5conf);

        Preconditions.checkState(!allNotSet, "xh's elasticsearch type of kerberos file is required");

        return true;
    }

}
