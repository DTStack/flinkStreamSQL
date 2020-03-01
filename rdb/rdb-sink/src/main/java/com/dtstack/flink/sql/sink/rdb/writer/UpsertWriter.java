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

package com.dtstack.flink.sql.sink.rdb.writer;

import com.dtstack.flink.sql.outputformat.DtRichOutputFormat;
import com.dtstack.flink.sql.sink.rdb.dialect.JDBCDialect;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.dtstack.flink.sql.sink.rdb.JDBCTypeConvertUtils.setRecordToStatement;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Upsert writer to deal with upsert, delete message.dd
 */
public abstract class UpsertWriter implements JDBCWriter {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(UpsertWriter.class);


    public static UpsertWriter create(
            JDBCDialect dialect,
            String schema,
            String tableName,
            String[] fieldNames,
            int[] fieldTypes,
            String[] keyFields,
            String[] partitionFields,
            boolean objectReuse,
            boolean allReplace,
            DtRichOutputFormat metricOutputFormat) {

        checkNotNull(keyFields);

        List<String> nameList = Arrays.asList(fieldNames);
        int[] pkFields = Arrays.stream(keyFields).mapToInt(nameList::indexOf).toArray();
        int[] pkTypes = fieldTypes == null ? null :
                Arrays.stream(pkFields).map(f -> fieldTypes[f]).toArray();

        String deleteSQL = dialect.getDeleteStatement(schema, tableName, keyFields);
        LOG.info("deleteSQL is :{}", deleteSQL);
        System.out.println("deleteSQL is :" + deleteSQL);

        Optional<String> upsertSQL = dialect.getUpsertStatement(schema, tableName, fieldNames, keyFields, allReplace);
        LOG.info("execute UpsertStatement: {}", upsertSQL.orElse("use UsingInsertUpdateStatement"));
        System.out.println("execute UpsertStatement: " + upsertSQL.orElse("use UsingInsertUpdateStatement"));

        return upsertSQL.map((Function<String, UpsertWriter>) sql ->
                new UpsertWriterUsingUpsertStatement(
                        fieldTypes, pkFields, pkTypes, objectReuse, deleteSQL, sql, metricOutputFormat))
                .orElseGet(() ->
                        new UpsertWriterUsingInsertUpdateStatement(
                                fieldTypes, pkFields, pkTypes, objectReuse, deleteSQL,
                                dialect.getRowExistsStatement(tableName, keyFields),
                                dialect.getInsertIntoStatement(schema, tableName, fieldNames, partitionFields),
                                dialect.getUpdateStatement(tableName, fieldNames, keyFields),
                                metricOutputFormat));
    }

    final int[] fieldTypes;
    final int[] pkTypes;
    private final int[] pkFields;
    private final String deleteSQL;
    private final boolean objectReuse;

    private transient Map<Row, Tuple2<Boolean, Row>> keyToRows;
    private transient PreparedStatement deleteStatement;
    // only use metric
    private transient DtRichOutputFormat metricOutputFormat;

    private UpsertWriter(int[] fieldTypes, int[] pkFields, int[] pkTypes, String deleteSQL, boolean objectReuse, DtRichOutputFormat metricOutputFormat) {
        this.fieldTypes = fieldTypes;
        this.pkFields = pkFields;
        this.pkTypes = pkTypes;
        this.deleteSQL = deleteSQL;
        this.objectReuse = objectReuse;
        this.metricOutputFormat = metricOutputFormat;
    }

    @Override
    public void open(Connection connection) throws SQLException {
        this.keyToRows = new HashMap<>();
        prepareStatement(connection);
    }

    @Override
    public void prepareStatement(Connection connection) throws SQLException {
        this.deleteStatement = connection.prepareStatement(deleteSQL);
    }

    @Override
    public void addRecord(Tuple2<Boolean, Row> record) throws SQLException {
        // we don't need perform a deep copy, because jdbc field are immutable object.
        Tuple2<Boolean, Row> tuple2 = objectReuse ? new Tuple2<>(record.f0, Row.copy(record.f1)) : record;
        // add records to buffer
        keyToRows.put(getPrimaryKey(tuple2.f1), tuple2);
    }

    @Override
    public void executeBatch(Connection connection) throws SQLException {
        try {
            if (keyToRows.size() > 0) {
                for (Map.Entry<Row, Tuple2<Boolean, Row>> entry : keyToRows.entrySet()) {
                    Row pk = entry.getKey();
                    Tuple2<Boolean, Row> tuple = entry.getValue();
                    if (tuple.f0) {
                        processOneRowInBatch(pk, tuple.f1);
                    } else {
                        setRecordToStatement(deleteStatement, pkTypes, pk);
                        deleteStatement.addBatch();
                    }
                }
                internalExecuteBatch();
                deleteStatement.executeBatch();
                connection.commit();
                keyToRows.clear();
            }
        } catch (Exception e) {
            // 清理批处理中的正确字段，防止重复写入
            connection.rollback();
            connection.commit();
            cleanBatchWhenError();
            executeUpdate(connection);
        }
    }

    @Override
    public void executeUpdate(Connection connection) throws SQLException {
        if (keyToRows.size() > 0) {
            for (Map.Entry<Row, Tuple2<Boolean, Row>> entry : keyToRows.entrySet()) {
                try {
                    Row pk = entry.getKey();
                    Tuple2<Boolean, Row> tuple = entry.getValue();
                    if (tuple.f0) {
                        processOneRowInBatch(pk, tuple.f1);
                        internalExecuteBatch();
                    } else {
                        setRecordToStatement(deleteStatement, pkTypes, pk);
                        deleteStatement.executeUpdate();
                    }
                    connection.commit();
                } catch (Exception e) {
                    System.out.println(e.getCause());
                    // deal pg error: current transaction is aborted, commands ignored until end of transaction block
                    connection.rollback();
                    connection.commit();
                    if (metricOutputFormat.outDirtyRecords.getCount() % DIRTYDATA_PRINT_FREQUENTY == 0 || LOG.isDebugEnabled()) {
                        LOG.error("record insert failed ,this row is {}", entry.getValue());
                        LOG.error("", e);
                    }
                    metricOutputFormat.outDirtyRecords.inc();
                }
            }
            keyToRows.clear();
        }
    }

    abstract void processOneRowInBatch(Row pk, Row row) throws SQLException;

    abstract void internalExecuteBatch() throws SQLException;

    @Override
    public void close() throws SQLException {
        if (deleteStatement != null) {
            deleteStatement.close();
            deleteStatement = null;
        }
    }

    private Row getPrimaryKey(Row row) {
        Row pks = new Row(pkFields.length);
        for (int i = 0; i < pkFields.length; i++) {
            pks.setField(i, row.getField(pkFields[i]));
        }
        return pks;
    }

    // ----------------------------------------------------------------------------------------

    private static final class UpsertWriterUsingUpsertStatement extends UpsertWriter {

        private static final long serialVersionUID = 1L;
        private final String upsertSQL;

        private transient PreparedStatement upsertStatement;

        private UpsertWriterUsingUpsertStatement(
                int[] fieldTypes,
                int[] pkFields,
                int[] pkTypes,
                boolean objectReuse,
                String deleteSQL,
                String upsertSQL,
                DtRichOutputFormat metricOutputFormat) {
            super(fieldTypes, pkFields, pkTypes, deleteSQL, objectReuse, metricOutputFormat);
            this.upsertSQL = upsertSQL;
        }

        @Override
        public void open(Connection connection) throws SQLException {
            super.open(connection);
        }

        @Override
        public void prepareStatement(Connection connection) throws SQLException {
            super.prepareStatement(connection);
            upsertStatement = connection.prepareStatement(upsertSQL);
        }

        @Override
        void processOneRowInBatch(Row pk, Row row) throws SQLException {
            setRecordToStatement(upsertStatement, fieldTypes, row);
            upsertStatement.addBatch();
        }

        @Override
        public void cleanBatchWhenError() throws SQLException {
            upsertStatement.clearBatch();
            upsertStatement.clearParameters();
        }

        @Override
        void internalExecuteBatch() throws SQLException {
            upsertStatement.executeBatch();
        }

        @Override
        public void close() throws SQLException {
            super.close();
            if (upsertStatement != null) {
                upsertStatement.close();
                upsertStatement = null;
            }
        }
    }

    private static final class UpsertWriterUsingInsertUpdateStatement extends UpsertWriter {

        private static final long serialVersionUID = 1L;
        private final String existSQL;
        private final String insertSQL;
        private final String updateSQL;

        private transient PreparedStatement existStatement;
        private transient PreparedStatement insertStatement;
        private transient PreparedStatement updateStatement;

        private UpsertWriterUsingInsertUpdateStatement(
                int[] fieldTypes,
                int[] pkFields,
                int[] pkTypes,
                boolean objectReuse,
                String deleteSQL,
                String existSQL,
                String insertSQL,
                String updateSQL,
                DtRichOutputFormat metricOutputFormat) {
            super(fieldTypes, pkFields, pkTypes, deleteSQL, objectReuse, metricOutputFormat);
            this.existSQL = existSQL;
            this.insertSQL = insertSQL;
            this.updateSQL = updateSQL;
        }

        @Override
        public void open(Connection connection) throws SQLException {
            super.open(connection);
        }

        @Override
        public void prepareStatement(Connection connection) throws SQLException {
            super.prepareStatement(connection);
            existStatement = connection.prepareStatement(existSQL);
            insertStatement = connection.prepareStatement(insertSQL);
            updateStatement = connection.prepareStatement(updateSQL);
        }

        @Override
        void processOneRowInBatch(Row pk, Row row) throws SQLException {
            setRecordToStatement(existStatement, pkTypes, pk);
            ResultSet resultSet = existStatement.executeQuery();
            boolean exist = resultSet.next();
            resultSet.close();
            if (exist) {
                // do update
                setRecordToStatement(updateStatement, fieldTypes, row);
                updateStatement.addBatch();
            } else {
                // do insert
                setRecordToStatement(insertStatement, fieldTypes, row);
                insertStatement.addBatch();
            }
        }

        @Override
        public void cleanBatchWhenError() throws SQLException {
            updateStatement.clearBatch();
            insertStatement.clearBatch();
        }

        @Override
        void internalExecuteBatch() throws SQLException {
            updateStatement.executeBatch();
            insertStatement.executeBatch();
        }

        @Override
        public void close() throws SQLException {
            super.close();
            if (existStatement != null) {
                existStatement.close();
                existStatement = null;
            }
            if (insertStatement != null) {
                insertStatement.close();
                insertStatement = null;
            }
            if (updateStatement != null) {
                updateStatement.close();
                updateStatement = null;
            }
        }
    }
}
