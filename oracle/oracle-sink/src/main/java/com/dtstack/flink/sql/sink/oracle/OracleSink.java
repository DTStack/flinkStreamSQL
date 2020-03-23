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
package com.dtstack.flink.sql.sink.oracle;

import com.dtstack.flink.sql.sink.IStreamSinkGener;
import com.dtstack.flink.sql.sink.rdb.JDBCOptions;
import com.dtstack.flink.sql.sink.rdb.RdbSink;
import com.dtstack.flink.sql.sink.rdb.format.JDBCUpsertOutputFormat;

/**
 * Reason:
 * Date: 2018/11/27
 * Company: www.dtstack.com
 *
 * @author maqi
 */
public class OracleSink extends RdbSink implements IStreamSinkGener<RdbSink> {

    private final String SQL_DEFAULT_PLACEHOLDER = " ? ";
    private final String DEAL_CHAR_KEY = "char";
    private String RPAD_FORMAT = " rpad(?, %d, ' ') ";

    public OracleSink() {
        super(new OracleDialect());
    }

    @Override
    public JDBCUpsertOutputFormat getOutputFormat() {
        JDBCOptions jdbcOptions = JDBCOptions.builder()
                .setDBUrl(dbURL)
                .setDialect(jdbcDialect)
                .setUsername(userName)
                .setPassword(password)
                .setTableName(tableName)
                .setSchema(schema)
                .build();

        return JDBCUpsertOutputFormat.builder()
                .setOptions(jdbcOptions)
                .setFieldNames(fieldNames)
                .setFlushMaxSize(batchNum)
                .setFlushIntervalMills(batchWaitInterval)
                .setFieldTypes(sqlTypes)
                .setKeyFields(primaryKeys)
                .setAllReplace(allReplace)
                .setUpdateMode(updateMode)
                .build();
    }
}
