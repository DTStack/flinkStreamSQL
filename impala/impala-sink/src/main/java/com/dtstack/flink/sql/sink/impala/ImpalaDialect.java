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

package com.dtstack.flink.sql.sink.impala;

import com.dtstack.flink.sql.sink.rdb.dialect.JDBCDialect;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Date: 2020/1/17
 * Company: www.dtstack.com
 * @author maqi
 */
public class ImpalaDialect implements JDBCDialect {
    private static final long serialVersionUID = 1L;

    private static final String IMPALA_PARTITION_KEYWORD = "partition";

    private TypeInformation[] fieldTypes;

    public ImpalaDialect(TypeInformation[] fieldTypes){
        this.fieldTypes = fieldTypes;
    }

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:impala:");
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("com.cloudera.impala.jdbc41.Driver");
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return identifier;
    }

    @Override
    public String getUpdateStatement(String tableName, String[] fieldNames, String[] conditionFields) {
        throw new RuntimeException("impala does not support update sql, please remove primary key or use append mode");
    }

    @Override
    public String getInsertIntoStatement(String schema, String tableName, String[] fieldNames, String[] partitionFields) {

        String schemaInfo = StringUtils.isEmpty(schema) ? "" : quoteIdentifier(schema) + ".";

        List<String> partitionFieldsList = Objects.isNull(partitionFields) ? Lists.newArrayList() : Arrays.asList(partitionFields);

        String columns = Arrays.stream(fieldNames)
                .filter(f -> !partitionFieldsList.contains(f))
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));

        String placeholders = Arrays.stream(fieldTypes)
                .map(f -> {
                    if(String.class.getName().equals(f.getTypeClass().getName())){
                        return "cast( ? as string)";
                    }
                    return "?";
                })
                .collect(Collectors.joining(", "));

        String partitionFieldStr = partitionFieldsList.stream()
                .map(field -> field.replaceAll("\"", "'"))
                .collect(Collectors.joining(", "));

        String partitionStatement = StringUtils.isEmpty(partitionFieldStr) ? "" : " " + IMPALA_PARTITION_KEYWORD + "(" + partitionFieldStr + ")";

        return "INSERT INTO " + schemaInfo + quoteIdentifier(tableName) +
                "(" + columns + ")" + partitionStatement + " VALUES (" + placeholders + ")";
    }
}
