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


package com.dtstack.flink.sql.sink.postgresql.table;

import com.dtstack.flink.sql.sink.rdb.table.RdbTableInfo;
import org.apache.flink.calcite.shaded.com.google.common.base.Preconditions;

/**
 * Date: 2019-08-22
 * Company: mmg
 *
 * @author tcm
 */

public class PostgresqlTableInfo extends RdbTableInfo {

    public static final String TABLE_IS_UPSERT = "isUpsert";

    public static final String TABLE_KEY_FIELD = "keyField";

    private static final String CURR_TYPE = "postgresql";

    private boolean isUpsert;

    private String keyField;


    public PostgresqlTableInfo() {
        setType(CURR_TYPE);
    }

    public boolean isUpsert() {
        return isUpsert;
    }

    public void setUpsert(boolean upsert) {
        isUpsert = upsert;
    }

    public String getKeyField() {
        return keyField;
    }

    public void setKeyField(String keyField) {
        this.keyField = keyField;
    }

    @Override
    public boolean check() {
        Preconditions.checkNotNull(getUrl(), "postgresql field of URL is required");
        Preconditions.checkNotNull(getTableName(), "postgresql field of tableName is required");
        Preconditions.checkNotNull(getUserName(), "postgresql field of userName is required");
        Preconditions.checkNotNull(getPassword(), "postgresql field of password is required");
        if (isUpsert()) {
            Preconditions.checkNotNull(getKeyField(), "postgresql field of keyField is required");
        }
        return true;
    }
}
