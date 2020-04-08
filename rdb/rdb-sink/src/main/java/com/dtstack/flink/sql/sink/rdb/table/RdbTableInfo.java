/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flink.sql.sink.rdb.table;

import com.dtstack.flink.sql.enums.EUpdateMode;
import com.dtstack.flink.sql.table.TargetTableInfo;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;

/**
 * Reason:
 * Date: 2018/11/27
 * Company: www.dtstack.com
 *
 * @author maqi
 */
public class RdbTableInfo extends TargetTableInfo {

    public static final String URL_KEY = "url";

    public static final String TABLE_NAME_KEY = "tableName";

    public static final String USER_NAME_KEY = "userName";

    public static final String PASSWORD_KEY = "password";

    public static final String BATCH_SIZE_KEY = "batchSize";

    public static final String BATCH_WAIT_INTERVAL_KEY = "batchWaitInterval";

    public static final String BUFFER_SIZE_KEY = "bufferSize";

    public static final String FLUSH_INTERVALMS_KEY = "flushIntervalMs";

    public static final String SCHEMA_KEY = "schema";

    public static final String ALLREPLACE_KEY = "allReplace";

    public static final String UPDATE_KEY = "updateMode";

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

    public Long getBatchWaitInterval() {
        return batchWaitInterval;
    }

    public void setBatchWaitInterval(Long batchWaitInterval) {
        this.batchWaitInterval = batchWaitInterval;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public boolean isAllReplace() {
        return allReplace;
    }

    public void setAllReplace(boolean allReplace) {
        this.allReplace = allReplace;
    }

    public String getUpdateMode() {
        return updateMode;
    }

    public void setUpdateMode(String updateMode) {
        this.updateMode = updateMode;
    }

    @Override
    public boolean check() {
        Preconditions.checkNotNull(url, "rdb field of URL is required");
        Preconditions.checkNotNull(tableName, "rdb field of tableName is required");
        Preconditions.checkNotNull(userName, "rdb field of userName is required");
        Preconditions.checkNotNull(password, "rdb field of password is required");

        if (StringUtils.equalsIgnoreCase(updateMode, EUpdateMode.UPSERT.name())) {
            Preconditions.checkArgument(null != getPrimaryKeys() && getPrimaryKeys().size() > 0, "updateMode  mode primary is required");
        }

        if (null != getPrimaryKeys()) {
            getPrimaryKeys().forEach(pk -> {
                Preconditions.checkArgument(getFieldList().contains(pk), "primary key " + pk + " not found in sink table field");
            });
        }


        Preconditions.checkArgument(getFieldList().size() == getFieldExtraInfoList().size(),
                "fields and fieldExtraInfoList attributes must be the same length");
        return true;
    }

    @Override
    public String getType() {
        // return super.getType().toLowerCase() + TARGET_SUFFIX;
        return super.getType().toLowerCase();
    }
}
