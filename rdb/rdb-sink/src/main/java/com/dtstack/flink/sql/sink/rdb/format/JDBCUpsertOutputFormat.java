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

package com.dtstack.flink.sql.sink.rdb.format;


import com.dtstack.flink.sql.enums.EUpdateMode;
import com.dtstack.flink.sql.factory.DTThreadFactory;
import com.dtstack.flink.sql.sink.rdb.JDBCOptions;
import com.dtstack.flink.sql.sink.rdb.dialect.JDBCDialect;
import com.dtstack.flink.sql.sink.rdb.writer.AppendOnlyWriter;
import com.dtstack.flink.sql.sink.rdb.writer.JDBCWriter;
import com.dtstack.flink.sql.sink.rdb.writer.AbstractUpsertWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An upsert OutputFormat for JDBC.
 *
 * @author maqi
 */
public class JDBCUpsertOutputFormat extends AbstractJDBCOutputFormat<Tuple2<Boolean, Row>> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(JDBCUpsertOutputFormat.class);

    static final int RECEIVEDATA_PRINT_FREQUENTY = 1000;

    private final String schema;
    private final String tableName;
    private final JDBCDialect dialect;
    private final String[] fieldNames;
    private final String[] keyFields;
    private final String[] partitionFields;
    private final int[] fieldTypes;

    private final int flushMaxSize;
    private final long flushIntervalMills;
    private final boolean allReplace;
    private final String updateMode;

    private transient JDBCWriter jdbcWriter;
    private transient int batchCount = 0;
    private transient volatile boolean closed = false;

    private transient ScheduledExecutorService scheduler;
    private transient ScheduledFuture scheduledFuture;
    private final AtomicBoolean flushFlag = new AtomicBoolean(true);

    public JDBCUpsertOutputFormat(
            JDBCOptions options,
            String[] fieldNames,
            String[] keyFields,
            String[] partitionFields,
            int[] fieldTypes,
            int flushMaxSize,
            long flushIntervalMills,
            boolean allReplace,
            String updateMode) {
        super(options.getUsername(), options.getPassword(), options.getDriverName(), options.getDbUrl());
        this.schema = options.getSchema();
        this.tableName = options.getTableName();
        this.dialect = options.getDialect();
        this.fieldNames = fieldNames;
        this.keyFields = keyFields;
        this.partitionFields = partitionFields;
        this.fieldTypes = fieldTypes;
        this.flushMaxSize = flushMaxSize;
        this.flushIntervalMills = flushIntervalMills;
        this.allReplace = allReplace;
        this.updateMode = updateMode;
    }

    /**
     * Connects to the target database and initializes the prepared statement.
     *
     * @param taskNumber The number of the parallel instance.
     * @throws IOException Thrown, if the output could not be opened due to an
     *                     I/O problem.
     */
    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        try {
            establishConnection();
            initMetric();

            if (StringUtils.equalsIgnoreCase(updateMode, EUpdateMode.APPEND.name()) || keyFields == null || keyFields.length == 0) {
                String insertSql = dialect.getInsertIntoStatement(schema, tableName, fieldNames, partitionFields);
                LOG.info("execute insert sql： {}", insertSql);
                jdbcWriter = new AppendOnlyWriter(insertSql, fieldTypes, this);
            } else {
                jdbcWriter = AbstractUpsertWriter.create(
                        dialect, schema, tableName, fieldNames, fieldTypes, keyFields, partitionFields,
                        getRuntimeContext().getExecutionConfig().isObjectReuseEnabled(), allReplace, this);
            }
            jdbcWriter.open(connection);
        } catch (SQLException sqe) {
            throw new IllegalArgumentException("open() failed.", sqe);
        } catch (ClassNotFoundException cnfe) {
            throw new IllegalArgumentException("JDBC driver class not found.", cnfe);
        }

        if (flushIntervalMills != 0) {
            this.scheduler = new ScheduledThreadPoolExecutor(1,
                    new DTThreadFactory("jdbc-upsert-output-format"));
            this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(() -> {
                synchronized (JDBCUpsertOutputFormat.this) {
                    if (closed) {
                        return;
                    }
                    try {
                        flush();
                    } catch (Exception e) {
                        flushFlag.set(false);
                        throw new RuntimeException("Writing records to JDBC failed.", e);
                    }
                }
            }, flushIntervalMills, flushIntervalMills, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public synchronized void writeRecord(Tuple2<Boolean, Row> tuple2) throws IOException {
        if(!flushFlag.get()){
            throw new RuntimeException("connect exception,can not write record");
        }
        checkConnectionOpen();
        try {
            if (outRecords.getCount() % RECEIVEDATA_PRINT_FREQUENTY == 0 || LOG.isDebugEnabled()) {
                LOG.info("Receive data : {}", tuple2);
            }
            // Receive data
            outRecords.inc();

            jdbcWriter.addRecord(tuple2);
            batchCount++;
            if (batchCount >= flushMaxSize) {
                flush();
            }
        } catch (Exception e) {
            throw new RuntimeException("Writing records to JDBC failed.", e);
        }
    }

    private void checkConnectionOpen() {
        try {
            if (!connection.isValid(10)) {
                LOG.info("db connection reconnect..");
                establishConnection();
                jdbcWriter.prepareStatement(connection);
            }
        } catch (SQLException e) {
            LOG.error("check connection open failed..", e);
        } catch (ClassNotFoundException e) {
            LOG.error("load jdbc class error when reconnect db..", e);
        } catch (IOException e) {
            LOG.error("jdbc io exception..", e);
        }
    }

    public synchronized void flush() throws Exception {
        jdbcWriter.executeBatch(connection);
        batchCount = 0;
        flushFlag.set(true);
    }

    /**
     * Executes prepared statement and closes all resources of this instance.
     *
     * @throws IOException Thrown, if the input could not be closed properly.
     */
    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        if (this.scheduledFuture != null) {
            scheduledFuture.cancel(false);
            this.scheduler.shutdown();
        }

        if (batchCount > 0) {
            try {
                flush();
            } catch (Exception e) {
                throw new RuntimeException("Writing records to JDBC failed.", e);
            }
        }

        try {
            jdbcWriter.close();
        } catch (SQLException e) {
            LOG.warn("Close JDBC writer failed.", e);
        }

        closeDbConnection();
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for a {@link JDBCUpsertOutputFormat}.
     */
    public static class Builder {
        protected JDBCOptions options;
        protected String[] fieldNames;
        protected String[] keyFields;
        protected String[] partitionFields;
        protected int[] fieldTypes;
        protected int flushMaxSize = DEFAULT_FLUSH_MAX_SIZE;
        protected long flushIntervalMills = DEFAULT_FLUSH_INTERVAL_MILLS;
        protected boolean allReplace = DEFAULT_ALLREPLACE_VALUE;
        protected String updateMode;

        /**
         * required, jdbc options.
         */
        public Builder setOptions(JDBCOptions options) {
            this.options = options;
            return this;
        }

        /**
         * required, field names of this jdbc sink.
         */
        public Builder setFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        /**
         * required, upsert unique keys.
         */
        public Builder setKeyFields(List<String> keyFields) {
            this.keyFields = keyFields == null ? null : keyFields.toArray(new String[keyFields.size()]);
            return this;
        }

        /**
         * required, field types of this jdbc sink.
         */
        public Builder setFieldTypes(int[] fieldTypes) {
            this.fieldTypes = fieldTypes;
            return this;
        }

        /**
         * optional, partition Fields
         *
         * @param partitionFields
         * @return
         */
        public Builder setPartitionFields(String[] partitionFields) {
            this.partitionFields = partitionFields;
            return this;
        }

        /**
         * optional, flush max size (includes all append, upsert and delete records),
         * over this number of records, will flush data.
         */
        public Builder setFlushMaxSize(int flushMaxSize) {
            this.flushMaxSize = flushMaxSize;
            return this;
        }

        /**
         * optional, flush interval mills, over this time, asynchronous threads will flush data.
         */
        public Builder setFlushIntervalMills(long flushIntervalMills) {
            this.flushIntervalMills = flushIntervalMills;
            return this;
        }

        public Builder setAllReplace(boolean allReplace) {
            this.allReplace = allReplace;
            return this;
        }

        public Builder setUpdateMode(String updateMode) {
            this.updateMode = updateMode;
            return this;
        }

        /**
         * Finalizes the configuration and checks validity.
         *
         * @return Configured JDBCUpsertOutputFormat
         */
        public JDBCUpsertOutputFormat build() {
            checkNotNull(options, "No options supplied.");
            checkNotNull(fieldNames, "No fieldNames supplied.");
            return new JDBCUpsertOutputFormat(
                    options, fieldNames, keyFields, partitionFields, fieldTypes, flushMaxSize, flushIntervalMills, allReplace, updateMode);
        }
    }
}
