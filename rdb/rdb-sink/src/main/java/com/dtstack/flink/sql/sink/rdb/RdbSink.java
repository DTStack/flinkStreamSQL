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
package com.dtstack.flink.sql.sink.rdb;

import com.dtstack.flink.sql.sink.IStreamSinkGener;
import com.dtstack.flink.sql.sink.rdb.table.RdbTableInfo;
import com.dtstack.flink.sql.table.TargetTableInfo;

import java.util.Arrays;
import java.util.List;

/**
 * Reason:
 * Date: 2018/11/27
 * Company: www.dtstack.com
 *
 * @author maqi
 */
public abstract class RdbSink extends DBSink implements IStreamSinkGener<RdbSink> {

    @Override
    public RdbSink genStreamSink(TargetTableInfo targetTableInfo) {
        RdbTableInfo rdbTableInfo = (RdbTableInfo) targetTableInfo;

        String tmpDbURL = rdbTableInfo.getUrl();
        String tmpUserName = rdbTableInfo.getUserName();
        String tmpPassword = rdbTableInfo.getPassword();
        String tmpTableName = rdbTableInfo.getTableName();

        Integer tmpSqlBatchSize = rdbTableInfo.getBatchSize();
        if (tmpSqlBatchSize != null) {
            setBatchInterval(tmpSqlBatchSize);
        }

        Integer tmpSinkParallelism = rdbTableInfo.getParallelism();
        if (tmpSinkParallelism != null) {
            setParallelism(tmpSinkParallelism);
        }

        List<String> fields = Arrays.asList(rdbTableInfo.getFields());
        List<Class> fieldTypeArray = Arrays.asList(rdbTableInfo.getFieldClasses());

        this.driverName = getDriverName();
        this.dbURL = tmpDbURL;
        this.userName = tmpUserName;
        this.password = tmpPassword;
        this.tableName = tmpTableName;
        this.primaryKeys = rdbTableInfo.getPrimaryKeys();
        buildSql(tableName, fields);
        buildSqlTypes(fieldTypeArray);
        return this;
    }

    public abstract String getDriverName();

}
