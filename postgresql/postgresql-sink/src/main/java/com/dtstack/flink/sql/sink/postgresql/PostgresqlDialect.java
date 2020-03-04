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

package com.dtstack.flink.sql.sink.postgresql;


import com.dtstack.flink.sql.sink.rdb.dialect.JDBCDialect;
import com.dtstack.flink.sql.util.DtStringUtil;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Date: 2020/1/3
 * Company: www.dtstack.com
 * @author maqi
 */
public class PostgresqlDialect implements JDBCDialect {
    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:postgresql:");
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("org.postgresql.Driver");
    }

    @Override
    public Optional<String> getUpsertStatement(String schema, String tableName, String[] fieldNames, String[] uniqueKeyFields, boolean allReplace) {
        String uniqueColumns = Arrays.stream(uniqueKeyFields)
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));

        String updateClause = Arrays.stream(fieldNames)
                .map(f -> quoteIdentifier(f) + "=EXCLUDED." + quoteIdentifier(f))
                .collect(Collectors.joining(", "));

        return Optional.of(getInsertIntoStatement(schema, tableName, fieldNames, null) +
                " ON CONFLICT (" + uniqueColumns + ")" +
                " DO UPDATE SET " + updateClause
        );

    }

}
