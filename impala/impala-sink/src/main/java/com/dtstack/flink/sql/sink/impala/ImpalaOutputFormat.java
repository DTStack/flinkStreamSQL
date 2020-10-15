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

import com.dtstack.flink.sql.factory.DTThreadFactory;
import com.dtstack.flink.sql.outputformat.AbstractDtRichOutputFormat;
import com.dtstack.flink.sql.sink.rdb.JDBCTypeConvertUtils;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.util.JDBCUtils;
import com.dtstack.flink.sql.util.KrbUtils;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.rmi.RemoteException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Date: 2020/10/14
 * Company: www.dtstack.com
 *
 * @author tiezhu
 */
public class ImpalaOutputFormat extends AbstractDtRichOutputFormat<Tuple2<Boolean, Row>> {

    private static final Logger LOG = LoggerFactory.getLogger(ImpalaOutputFormat.class);

    private static final long serialVersionUID = 1L;

    // ${field}
    private static final Pattern STATIC_PARTITION_PATTERN = Pattern.compile("\\$\\{([^}]*)}");

    private static final Integer DEFAULT_CONN_TIME_OUT = 60;
    private static final int RECEIVE_DATA_PRINT_FREQUENCY = 1000;

    private static final String KUDU_TYPE = "kudu";
    private static final String UPDATE_MODE = "update";
    private static final String PARTITION_CONSTANT = "PARTITION";
    private static final String STRING_TYPE = "STRING";
    private static final String PARTITION_CONDITION = "$partitionCondition";
    private static final String DRIVER_NAME = "com.cloudera.impala.jdbc41.Driver";

    protected transient Connection connection;
    protected transient PreparedStatement statement;

    private transient volatile boolean closed = false;
    private final AtomicInteger batchCount = new AtomicInteger(0);

    protected String keytabPath;
    protected String krb5confPath;
    protected String principal;
    protected Integer authMech;
    protected String dbUrl;
    protected String userName;
    protected String password;
    protected int batchSize = 100;
    protected long batchWaitInterval = 60 * 1000L;
    protected String tableName;
    protected List<String> primaryKeys;
    protected String partitionFields;
    protected Boolean enablePartition;
    protected String schema;
    protected String storeType;
    protected String partitionMode;
    protected String updateMode;
    public List<String> fieldList;
    public List<String> fieldTypeList;
    public List<AbstractTableInfo.FieldExtraInfo> fieldExtraInfoList;

    // partition field of static partition which matched by ${field}
    private final List<String> staticPartitionField = new ArrayList<>();

    // static partition sql like 'INSERT INTO tableName(field1, field2) PARTITION(pt=xx) VALUES(?, ?)'
    private String staticPartitionSql = "";

    private transient ScheduledExecutorService scheduler;
    private transient ScheduledFuture<?> scheduledFuture;

    @Override
    public void configure(Configuration parameters) {
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        openConnect();
        initScheduledTask(batchWaitInterval);
        initMetric();
    }

    private void initScheduledTask(Long batchWaitInterval) {
        if (batchWaitInterval != 0) {
            this.scheduler = new ScheduledThreadPoolExecutor(1,
                    new DTThreadFactory("impala-upsert-output-format"));
            this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(() -> {
                synchronized (ImpalaOutputFormat.this) {
                    if (closed) {
                        return;
                    }
                    try {
                        flush();
                    } catch (Exception e) {
                        LOG.error("Writing records to impala jdbc failed.", e);
                        throw new RuntimeException("Writing records to impala jdbc failed.", e);
                    }
                }
            }, batchWaitInterval, batchWaitInterval, TimeUnit.MILLISECONDS);
        }
    }

    private void openConnect() throws IOException {
        if (authMech == 1) {
            UserGroupInformation ugi = KrbUtils.getUgi(principal, keytabPath, krb5confPath);
            try {
                ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
                    openJdbc();
                    return null;
                });
            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
            }
        } else {
            openJdbc();
        }
    }

    /**
     * get jdbc connection and create statement
     */
    private void openJdbc() {
        JDBCUtils.forName(DRIVER_NAME, getClass().getClassLoader());
        try {
            connection = DriverManager.getConnection(dbUrl, userName, password);
            connection.setAutoCommit(false);

            //update mode
            if (updateMode.equalsIgnoreCase(UPDATE_MODE)) {
                statement = connection.prepareStatement(
                        buildUpdateSql(schema, tableName, fieldList, primaryKeys)
                );
                return;
            }

            // kudu
            if (storeType.equalsIgnoreCase(KUDU_TYPE)) {
                statement = connection.prepareStatement(
                        buildKuduInsertSql(schema, tableName, fieldList, fieldTypeList)
                );
                return;
            }

            // match ${field} from partitionFields
            Matcher matcher = STATIC_PARTITION_PATTERN.matcher(partitionFields);
            while (matcher.find()) {
                staticPartitionField.add(matcher.group(1));
            }

            // dynamic
            if (enablePartition && staticPartitionField.isEmpty()) {
                statement = connection.prepareStatement(
                        buildDynamicInsertSql(schema, tableName, fieldList, fieldTypeList, partitionFields)
                );
            }
            // static
            if (enablePartition && !staticPartitionField.isEmpty()) {
                staticPartitionSql = buildStaticInsertSql(schema, tableName, fieldList, fieldTypeList, partitionFields);
            }

        } catch (SQLException sqlException) {
            throw new RuntimeException("get impala jdbc connection failed!");
        }
    }

    private void flush() throws SQLException {
        if (Objects.nonNull(statement)) {
            statement.executeBatch();
            batchCount.set(0);
        }
    }

    /**
     * 通过impala写入数据，具体分三种情况
     * 1.kudu 表 -> buildKuduInsertSql();
     * 2.静态分区的方式 -> buildStaticInsertSql();
     * 3.动态分区的方式 -> buildDynamicInsertSql();
     * 静态分区的方式中，分区字段值需要通过record中对应字段来获取填充
     *
     * @param record 回撤流数据
     * @throws IOException IOException
     */
    @Override
    public void writeRecord(Tuple2<Boolean, Row> record) throws IOException {
        try {
            Map<String, Object> valueMap = Maps.newHashMap();

            if (outRecords.getCount() % RECEIVE_DATA_PRINT_FREQUENCY == 0 || LOG.isDebugEnabled()) {
                LOG.info("Receive data : {}", record);
            }
            // Receive data
            outRecords.inc();

            Row copyRow = Row.copy(record.f1);

            for (int i = 0; i < copyRow.getArity(); i++) {
                valueMap.put(fieldList.get(i), copyRow.getField(i));
            }

            // build static partition statement from row data
            if (Objects.isNull(statement) || !staticPartitionSql.isEmpty()) {
                statement = connection.prepareStatement(
                        staticPartitionSql.replace(PARTITION_CONDITION,
                                buildStaticPartitionCondition(valueMap, staticPartitionField))
                );
            }

            Row rowValue = new Row(fieldTypeList.size());
            for (int i = 0; i < fieldTypeList.size(); i++) {
                rowValue.setField(i, copyRow.getField(i));
            }

            if (updateMode.equalsIgnoreCase(UPDATE_MODE)) {
                setRowToStatement(statement, fieldTypeList, rowValue, primaryKeys.stream().mapToInt(fieldList::indexOf).toArray());
            } else {
                setRowToStatement(statement, fieldTypeList, rowValue);
            }

            statement.addBatch();

            if (batchCount.incrementAndGet() > batchSize) {
                flush();
            }
        } catch (Exception e) {
            throw new RuntimeException("Writing records to impala failed.", e);
        }
    }

    private void setRowToStatement(PreparedStatement statement, List<String> fieldTypeList, Row row) throws SQLException {
        JDBCTypeConvertUtils.setRecordToStatement(statement, JDBCTypeConvertUtils.getSqlTypeFromFieldType(fieldTypeList), row);
    }

    private void setRowToStatement(PreparedStatement statement, List<String> fieldTypeList, Row row, int[] pkFields) throws SQLException {
        JDBCTypeConvertUtils.setRecordToStatement(statement, JDBCTypeConvertUtils.getSqlTypeFromFieldType(fieldTypeList), row, pkFields);
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        // cancel scheduled task
        if (this.scheduledFuture != null) {
            scheduledFuture.cancel(false);
            this.scheduler.shutdown();
        }
        // 将还未执行的SQL flush
        if (batchCount.get() > 0) {
            try {
                flush();
            } catch (Exception e) {
                throw new RuntimeException("Writing records to impala failed.", e);
            }
        }
        // close connection
        try {
            if (connection != null && connection.isValid(DEFAULT_CONN_TIME_OUT)) {
                connection.close();
            }
        } catch (SQLException e) {
            throw new RemoteException("impala connection close failed!");
        } finally {
            connection = null;
        }
        closed = true;
    }

    /**
     * impala 写入 kudu 表SQL
     *
     * @param schema     schema
     * @param tableName  tableName
     * @param fieldList  fieldList
     * @param fieldTypes fieldTypes
     * @return INSERT INTO kuduTable(field1, fields2) VALUES ( v1, v2)
     */
    private String buildKuduInsertSql(String schema,
                                      String tableName,
                                      List<String> fieldList,
                                      List<String> fieldTypes) {

        String columns = fieldList.stream()
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));

        String placeholders = fieldTypes.stream().map(
                f -> {
                    if (STRING_TYPE.equals(f.toUpperCase())) {
                        return "cast(? as string)";
                    }
                    return "?";
                }).collect(Collectors.joining(", "));

        return "INSERT INTO " + (Objects.isNull(schema) ? "" : quoteIdentifier(schema) + ".") + quoteIdentifier(tableName) +
                "(" + columns + ")" + " VALUES (" + placeholders + ")";
    }

    /**
     * get static partition value from rowData and build static partition condition
     *
     * @param rowData              the row data
     * @param staticPartitionField static partition field
     * @return partition condition like pt1=v1, pt2=v2
     */
    private String buildStaticPartitionCondition(Map<String, Object> rowData, List<String> staticPartitionField) {
        StringBuilder sb = new StringBuilder();
        for (String key : staticPartitionField) {
            Object value = rowData.get(key);
            sb.append(key).append("=").append(value);
        }
        return sb.toString();
    }

    /**
     * impala 通过静态分区的方式写入数据 SQL
     *
     * @return INSERT INTO tableName(field1, field2) PARTITION($partitionCondition) VALUES (?, ?)
     */
    private String buildStaticInsertSql(String schema, String tableName, List<String> fieldNames, List<String> fieldTypes, String partitionFields) {
        List<String> copyFieldNames = new ArrayList<>(fieldNames);
        for (int i = fieldNames.size() - 1; i >= 0; i--) {
            if (partitionFields.contains(fieldNames.get(i))) {
                copyFieldNames.remove(i);
                fieldTypes.remove(i);
            }
        }

        String placeholders = fieldTypes.stream().map(
                f -> {
                    if (STRING_TYPE.equals(f.toUpperCase())) {
                        return "cast(? as string)";
                    }
                    return "?";
                }).collect(Collectors.joining(", "));

        String columns = copyFieldNames.stream()
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));

        String partitionCondition = PARTITION_CONSTANT + "(" + PARTITION_CONDITION + ")";

        return "INSERT INTO " + (Objects.isNull(schema) ? "" : quoteIdentifier(schema) + ".") + quoteIdentifier(tableName) +
                " (" + columns + ") " + partitionCondition + " VALUES (" + placeholders + ")";
    }

    /**
     * impala 通过动态分区的方式写入数据 SQL
     *
     * @return INSERT INTO tableName(field1, field2) PARTITION(pt) VALUES (?, ?)
     */
    private String buildDynamicInsertSql(String schema, String tableName, List<String> fieldName, List<String> fieldTypes, String partitionFields) {

        String placeholders = fieldTypes.stream().map(
                f -> {
                    if (STRING_TYPE.equals(f.toUpperCase())) {
                        return "cast(? as string)";
                    }
                    return "?";
                }).collect(Collectors.joining(", "));

        String columns = fieldName.stream()
                .filter(f -> !partitionFields.contains(f))
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", "));

        String partitionCondition = PARTITION_CONSTANT + "(" + partitionFields + ")";

        return "INSERT INTO " + (Objects.isNull(schema) ? "" : quoteIdentifier(schema) + ".") + quoteIdentifier(tableName) +
                " (" + columns + ") " + partitionCondition + " VALUES (" + placeholders + ")";
    }

    /**
     * impala update mode SQL
     *
     * @return UPDATE tableName SET setCondition WHERE whereCondition
     */
    private String buildUpdateSql(String schema, String tableName, List<String> fieldNames, List<String> primaryKeys) {
        //跳过primary key字段
        String setClause = fieldNames.stream()
                .filter(f -> !CollectionUtils.isNotEmpty(primaryKeys) || !primaryKeys.contains(f))
                .map(f -> quoteIdentifier(f) + "=?")
                .collect(Collectors.joining(", "));

        String conditionClause = primaryKeys.stream()
                .map(f -> quoteIdentifier(f) + "=?")
                .collect(Collectors.joining(" AND "));

        return "UPDATE " + (Objects.isNull(schema) ? "" : quoteIdentifier(schema) + ".")
                + quoteIdentifier(tableName) + " SET " + setClause + " WHERE " + conditionClause;
    }

    public String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }

    public static Builder getImpalaBuilder() {
        return new Builder();
    }

    public static class Builder {
        private final ImpalaOutputFormat format = new ImpalaOutputFormat();

        public Builder setDbUrl(String dbUrl) {
            format.dbUrl = dbUrl;
            return this;
        }

        public Builder setUserName(String userName) {
            format.userName = userName;
            return this;
        }

        public Builder setPassword(String password) {
            format.password = password;
            return this;
        }

        public Builder setBatchSize(Integer batchSize) {
            format.batchSize = batchSize;
            return this;
        }

        public Builder setBatchWaitInterval(Long batchWaitInterval) {
            format.batchWaitInterval = batchWaitInterval;
            return this;
        }

        public Builder setTableName(String tableName) {
            format.tableName = tableName;
            return this;
        }

        public Builder setPartitionFields(String partitionFields) {
            format.partitionFields = partitionFields;
            return this;
        }

        public Builder setPrimaryKeys(List<String> primaryKeys) {
            format.primaryKeys = primaryKeys;
            return this;
        }

        public Builder setSchema(String schema) {
            format.schema = schema;
            return this;
        }

        public Builder setEnablePartition(Boolean enablePartition) {
            format.enablePartition = enablePartition;
            return this;
        }

        public Builder setUpdateMode(String updateMode) {
            format.updateMode = updateMode;
            return this;
        }

        public Builder setFieldList(List<String> fieldList) {
            format.fieldList = fieldList;
            return this;
        }

        public Builder setFieldTypeList(List<String> fieldTypeList) {
            format.fieldTypeList = fieldTypeList;
            return this;
        }

        public Builder setStoreType(String storeType) {
            format.storeType = storeType;
            return this;
        }

        public Builder setPartitionMode(String partitionMode) {
            format.partitionMode = partitionMode;
            return this;
        }

        public Builder setFieldExtraInfoList(List<AbstractTableInfo.FieldExtraInfo> fieldExtraInfoList) {
            format.fieldExtraInfoList = fieldExtraInfoList;
            return this;
        }

        public Builder setKeyTabPath(String keyTabPath) {
            format.keytabPath = keyTabPath;
            return this;
        }

        public Builder setKrb5ConfPath(String krb5ConfPath) {
            format.krb5confPath = krb5ConfPath;
            return this;
        }

        public Builder setPrincipal(String principal) {
            format.principal = principal;
            return this;
        }

        public Builder setAuthMech(Integer authMech) {
            format.authMech = authMech;
            return this;
        }

        private boolean canHandle(String url) {
            return url.startsWith("jdbc:impala:");
        }

        public ImpalaOutputFormat build() {
            if (!canHandle(format.dbUrl)) {
                throw new IllegalArgumentException("impala dbUrl is illegal, check url: " + format.dbUrl);
            }

            if (format.authMech == EAuthMech.Kerberos.getType()) {
                checkNotNull(format.krb5confPath,
                        "When kerberos authentication is enabled, krb5confPath is required！");
                checkNotNull(format.principal,
                        "When kerberos authentication is enabled, principal is required！");
                checkNotNull(format.keytabPath,
                        "When kerberos authentication is enabled, keytabPath is required！");
            }

            if (format.authMech == EAuthMech.UserName.getType()) {
                checkNotNull(format.userName, "userName is required!");
            }

            if (format.authMech == EAuthMech.NameANDPassword.getType()) {
                checkNotNull(format.userName, "userName is required!");
                checkNotNull(format.password, "password is required!");
            }

            return format;
        }

    }

}
