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

package com.dtstack.flink.sql.sink.kingbase;

import com.dtstack.flink.sql.sink.rdb.dialect.JDBCDialect;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Date: 2020/09/10
 * Company: www.dtstack.com
 *
 * @author tiezhu
 */
public class KingbaseDialect implements JDBCDialect {
    private static final long serialVersionUID = -8548141999909854264L;

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:kingbase");
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("com.kingbase8.Driver");
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }

    @Override
    public Optional<String> getUpsertStatement(String schema,
                                               String tableName,
                                               String[] fieldNames,
                                               String[] uniqueKeyFields,
                                               boolean allReplace) {
        return buildDuplicateUpsertStatement(tableName, fieldNames, uniqueKeyFields);
    }

    public Optional<String> buildDuplicateUpsertStatement(String tableName,
                                                          String[] fieldNames,
                                                          String[] uniqueKeyFields) {
        String updateClause = Arrays.stream(fieldNames)
                .map(f -> quoteIdentifier(f) + "=IFNULL(VALUES(" + quoteIdentifier(f) + ")," + quoteIdentifier(f) + ")")
                .collect(Collectors.joining(", "));

        return Optional.of(getInsertIntoStatement("", tableName, fieldNames, null) +
                " ON CONFLICT (" + Arrays.toString(uniqueKeyFields) + ") DO UPDATE SET " + updateClause
        );
    }
}
