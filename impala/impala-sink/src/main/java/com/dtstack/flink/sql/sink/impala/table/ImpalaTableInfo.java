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

import com.dtstack.flink.sql.enums.EUpdateMode;
import com.dtstack.flink.sql.table.AbstractTargetTableInfo;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

/**
 * Date: 2020/10/14
 * Company: www.dtstack.com
 *
 * @author tiezhu
 */
public class ImpalaTableInfo extends AbstractTargetTableInfo {

    private static final int MAX_BATCH_SIZE = 10000;

    private String url;

    private String tableName;

    private String userName;

    private String password;

    private Integer batchSize;

    private Long batchWaitInterval;

    private String bufferSize;

    private String flushIntervalMs;

    private String schema;

    private boolean allReplace;

    private String updateMode;

    private Integer authMech;

    private String krb5FilePath;

    private String principal;

    private String keyTabFilePath;

    private String krbRealm;

    private String krbHostFQDN;

    private String krbServiceName;

    private boolean enablePartition = false;

    private String partitionFields;

    private String storeType;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
    }

    public Long getBatchWaitInterval() {
        return batchWaitInterval;
    }

    public void setBatchWaitInterval(Long batchWaitInterval) {
        this.batchWaitInterval = batchWaitInterval;
    }

    public String getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(String bufferSize) {
        this.bufferSize = bufferSize;
    }

    public String getFlushIntervalMs() {
        return flushIntervalMs;
    }

    public void setFlushIntervalMs(String flushIntervalMs) {
        this.flushIntervalMs = flushIntervalMs;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getUpdateMode() {
        return updateMode;
    }

    public void setUpdateMode(String updateMode) {
        this.updateMode = updateMode;
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

    public boolean isEnablePartition() {
        return enablePartition;
    }

    public void setEnablePartition(boolean enablePartition) {
        this.enablePartition = enablePartition;
    }

    public String getPartitionFields() {
        return partitionFields;
    }

    public void setPartitionFields(String partitionFields) {
        this.partitionFields = partitionFields;
    }

    public String getStoreType() {
        return storeType;
    }

    public void setStoreType(String storeType) {
        this.storeType = storeType;
    }

    @Override
    public String getType() {
        return super.getType().toLowerCase();
    }

    @Override
    public boolean check() {
        Preconditions.checkNotNull(url, "impala field of URL is required");
        Preconditions.checkNotNull(tableName, "impala field of tableName is required");

        if (Objects.nonNull(batchSize)) {
            Preconditions.checkArgument(batchSize <= MAX_BATCH_SIZE, "batchSize must be less than " + MAX_BATCH_SIZE);
        }

        if (StringUtils.equalsIgnoreCase(updateMode, EUpdateMode.UPSERT.name())) {
            Preconditions.checkArgument(Objects.nonNull(getPrimaryKeys()) && getPrimaryKeys().size() > 0, "updateMode mode primary is required");
        }

        if (Objects.nonNull(getPrimaryKeys())) {
            getPrimaryKeys().forEach(pk -> {
                Preconditions.checkArgument(getFieldList().contains(pk), "primary key " + pk + " not found in sink table field");
            });
        }


        Preconditions.checkArgument(getFieldList().size() == getFieldExtraInfoList().size(),
                "fields and fieldExtraInfoList attributes must be the same length");
        return true;
    }
}
