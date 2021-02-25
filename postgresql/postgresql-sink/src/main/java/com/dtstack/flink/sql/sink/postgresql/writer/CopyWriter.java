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
package com.dtstack.flink.sql.sink.postgresql.writer;

import com.dtstack.flink.sql.outputformat.AbstractDtRichOutputFormat;
import com.dtstack.flink.sql.sink.rdb.writer.JDBCWriter;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * Company: www.dtstack.com
 * support pg copy mode
 *
 * @author dapeng
 * @date 2020-09-21
 */
public class CopyWriter implements JDBCWriter {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(CopyWriter.class);

    private static final String COPY_SQL_TEMPL = "copy %s(%s) from stdin DELIMITER '%s' NULL as '%s'";

    private static final String DEFAULT_FIELD_DELIM = "\001";

    private static final String DEFAULT_NULL_DELIM = "\002";

    private static final String LINE_DELIMITER = "\n";

    private CopyManager copyManager;

    private transient List<Row> rows;

    private String copySql;

    private String tableName;

    private String[] fieldNames;

    /**
     * dirty data count limit. Once count over limit then throw exception.
     */
    private long errorLimit;

    // only use metric
    private transient AbstractDtRichOutputFormat metricOutputFormat;

    public CopyWriter(String tableName, String[] fieldNames, AbstractDtRichOutputFormat metricOutputFormat, long errorLimit) {
        this.tableName = tableName;
        this.fieldNames = fieldNames;
        this.metricOutputFormat = metricOutputFormat;
        this.copySql = String.format(COPY_SQL_TEMPL, tableName, String.join(",", fieldNames), DEFAULT_FIELD_DELIM, DEFAULT_NULL_DELIM);
        this.errorLimit = errorLimit;
    }

    @Override
    public void initMetricOutput(AbstractDtRichOutputFormat metricOutputFormat) {
        this.metricOutputFormat = metricOutputFormat;
    }

    @Override
    public void open(Connection connection) throws SQLException {
        copyManager = new CopyManager((BaseConnection) connection);
        this.rows = Lists.newArrayList();
    }

    @Override
    public void prepareStatement(Connection connection) throws SQLException {

    }

    @Override
    public void addRecord(Tuple2<Boolean, Row> record) {
        if (!record.f0) {
            return;
        }
        rows.add(Row.copy(record.f1));
    }

    @Override
    public void executeBatch(Connection connection) throws SQLException {
        if(CollectionUtils.isEmpty(rows)){
            return;
        }
        //write with copy
        StringBuilder sb = new StringBuilder();
        for (Row row : rows) {
            int lastIndex = row.getArity() - 1;
            for (int index = 0; index < row.getArity(); index++) {
                Object rowData = row.getField(index);
                sb.append(rowData == null ? DEFAULT_NULL_DELIM : rowData);
                if (index != lastIndex) {
                    sb.append(DEFAULT_FIELD_DELIM);
                }
            }
            sb.append(LINE_DELIMITER);
        }
        try {
            ByteArrayInputStream bi = new ByteArrayInputStream(sb.toString().getBytes(StandardCharsets.UTF_8));
            copyManager.copyIn(copySql, bi);
            connection.commit();
            rows.clear();
        } catch (Exception e) {
            connection.rollback();
            connection.commit();
            executeUpdate(connection);
        }

    }

    @Override
    public void cleanBatchWhenError() throws SQLException {

    }

    @Override
    public void executeUpdate(Connection connection) throws SQLException {
        int index = 0;
        StringBuilder sb = new StringBuilder();
        for (Row row : rows) {
            try {
                for (; index < row.getArity(); index++) {
                    Object rowData = row.getField(index);
                    sb.append(rowData)
                            .append(DEFAULT_FIELD_DELIM);
                }

                String rowVal = sb.toString();
                ByteArrayInputStream bi = new ByteArrayInputStream(rowVal.getBytes(StandardCharsets.UTF_8));
                copyManager.copyIn(copySql, bi);
                connection.commit();
            } catch (Exception e) {
                dealExecuteError(
                    connection,
                    e,
                    metricOutputFormat,
                    row,
                    errorLimit,
                    LOG
                );
            }
        }
        rows.clear();

    }

    @Override
    public void close() throws SQLException {

    }

}
