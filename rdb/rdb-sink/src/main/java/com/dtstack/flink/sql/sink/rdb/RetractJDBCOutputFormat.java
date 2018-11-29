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

package com.dtstack.flink.sql.sink.rdb;

import com.dtstack.flink.sql.enums.EDatabaseType;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.*;
import java.util.*;
import java.io.IOException;

import com.dtstack.flink.sql.sink.MetricOutputFormat;

/**
 * OutputFormat to write tuples into a database.
 * The OutputFormat has to be configured using the supplied OutputFormatBuilder.
 *
 * @see Tuple
 * @see DriverManager
 */
public class RetractJDBCOutputFormat extends MetricOutputFormat {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RetractJDBCOutputFormat.class);

    private String username;
    private String password;
    private String drivername;
    private String dbURL;
    private String insertQuery;
    private String tableName;
    private String dbType;
    private int batchInterval = 5000;

    private Connection dbConn;
    private PreparedStatement upload;
    //index field
    private Map<String, List<String>> realIndexes = Maps.newHashMap();
    //full field
    private List<String> fullField = Lists.newArrayList();

    private DBSink dbSink;

    private int batchCount = 0;

    public int[] typesArray;

    private final static String GET_ORACLE_INDEX_SQL = "SELECT " +
            "t.INDEX_NAME," +
            "t.COLUMN_NAME " +
            "FROM " +
            "user_ind_columns t," +
            "user_indexes i " +
            "WHERE " +
            "t.index_name = i.index_name " +
            "AND i.uniqueness = 'UNIQUE' " +
            "AND t.table_name = '%s'";

    public RetractJDBCOutputFormat() {

    }

    @Override
    public void configure(Configuration parameters) {
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
            if (dbConn.getMetaData().getTables(null, null, tableName, null).next()) {
                if (!EDatabaseType.MYSQL.name().equalsIgnoreCase(dbType) && isReplaceInsertQuery()) {
                    insertQuery = dbSink.buildUpdateSql(tableName, Arrays.asList(dbSink.fieldNames), realIndexes, fullField);
                }
                upload = dbConn.prepareStatement(insertQuery);
            } else {
                throw new SQLException("Table " + tableName + " doesn't exist");
            }

        } catch (SQLException sqe) {
            throw new IllegalArgumentException("open() failed.", sqe);
        } catch (ClassNotFoundException cnfe) {
            throw new IllegalArgumentException("JDBC driver class not found.", cnfe);
        }
    }

    private boolean isReplaceInsertQuery() throws SQLException {
        getRealIndexes();
        getFullColumns();

        if (!realIndexes.isEmpty()) {
            for (List<String> value : realIndexes.values()) {
                for (String fieldName : dbSink.getFieldNames()) {
                    if (value.contains(fieldName)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private void establishConnection() throws SQLException, ClassNotFoundException {
        Class.forName(drivername);
        if (username == null) {
            dbConn = DriverManager.getConnection(dbURL);
        } else {
            dbConn = DriverManager.getConnection(dbURL, username, password);
        }
    }

    /**
     * Adds a record to the prepared statement.
     * <p>
     * When this method is called, the output format is guaranteed to be opened.
     * </p>
     * <p>
     * WARNING: this may fail when no column types specified (because a best effort approach is attempted in order to
     * insert a null value but it's not guaranteed that the JDBC driver handles PreparedStatement.setObject(pos, null))
     *
     * @param tuple2 The records to add to the output.
     * @throws IOException Thrown, if the records could not be added due to an I/O problem.
     * @see PreparedStatement
     */
    @Override
    public void writeRecord(Tuple2 tuple2) throws IOException {

        Tuple2<Boolean, Row> tupleTrans = tuple2;
        Boolean retract = tupleTrans.getField(0);
        Row row = tupleTrans.getField(1);


        if (typesArray != null && typesArray.length > 0 && typesArray.length != row.getArity()) {
            LOG.warn("Column SQL types array doesn't match arity of passed Row! Check the passed array...");
        }
        try {
            if (retract) {
                insertWrite(row);
                outRecords.inc();
            } else {
                //do nothing
            }
        } catch (SQLException | IllegalArgumentException e) {
            throw new IllegalArgumentException("writeRecord() failed", e);
        }
    }


    private void insertWrite(Row row) throws SQLException {

        updatePreparedStmt(row, upload);
        upload.addBatch();
        batchCount++;
        if (batchCount >= batchInterval) {
            upload.executeBatch();
            batchCount = 0;
        }
    }

    private void updatePreparedStmt(Row row, PreparedStatement pstmt) throws SQLException {
        if (typesArray == null) {
            // no types provided
            for (int index = 0; index < row.getArity(); index++) {
                LOG.warn("Unknown column type for column %s. Best effort approach to set its value: %s.", index + 1, row.getField(index));
                pstmt.setObject(index + 1, row.getField(index));
            }
        } else {
            // types provided
            for (int index = 0; index < row.getArity(); index++) {

                if (row.getField(index) == null) {
                    pstmt.setNull(index + 1, typesArray[index]);
                } else {
                    // casting values as suggested by http://docs.oracle.com/javase/1.5.0/docs/guide/jdbc/getstart/mapping.html
                    switch (typesArray[index]) {
                        case java.sql.Types.NULL:
                            pstmt.setNull(index + 1, typesArray[index]);
                            break;
                        case java.sql.Types.BOOLEAN:
                        case java.sql.Types.BIT:
                            pstmt.setBoolean(index + 1, (boolean) row.getField(index));
                            break;
                        case java.sql.Types.CHAR:
                        case java.sql.Types.NCHAR:
                        case java.sql.Types.VARCHAR:
                        case java.sql.Types.LONGVARCHAR:
                        case java.sql.Types.LONGNVARCHAR:
                            pstmt.setString(index + 1, (String) row.getField(index));
                            break;
                        case java.sql.Types.TINYINT:
                            pstmt.setByte(index + 1, (byte) row.getField(index));
                            break;
                        case java.sql.Types.SMALLINT:
                            pstmt.setShort(index + 1, (short) row.getField(index));
                            break;
                        case java.sql.Types.INTEGER:
                            pstmt.setInt(index + 1, (int) row.getField(index));
                            break;
                        case java.sql.Types.BIGINT:
                            pstmt.setLong(index + 1, (long) row.getField(index));
                            break;
                        case java.sql.Types.REAL:
                        case java.sql.Types.FLOAT:
                            pstmt.setFloat(index + 1, (float) row.getField(index));
                            break;
                        case java.sql.Types.DOUBLE:
                            pstmt.setDouble(index + 1, (double) row.getField(index));
                            break;
                        case java.sql.Types.DECIMAL:
                        case java.sql.Types.NUMERIC:
                            pstmt.setBigDecimal(index + 1, (java.math.BigDecimal) row.getField(index));
                            break;
                        case java.sql.Types.DATE:
                            pstmt.setDate(index + 1, (java.sql.Date) row.getField(index));
                            break;
                        case java.sql.Types.TIME:
                            pstmt.setTime(index + 1, (java.sql.Time) row.getField(index));
                            break;
                        case java.sql.Types.TIMESTAMP:
                            pstmt.setTimestamp(index + 1, (java.sql.Timestamp) row.getField(index));
                            break;
                        case java.sql.Types.BINARY:
                        case java.sql.Types.VARBINARY:
                        case java.sql.Types.LONGVARBINARY:
                            pstmt.setBytes(index + 1, (byte[]) row.getField(index));
                            break;
                        default:
                            pstmt.setObject(index + 1, row.getField(index));
                            LOG.warn("Unmanaged sql type (%s) for column %s. Best effort approach to set its value: %s.",
                                    typesArray[index], index + 1, row.getField(index));
                            // case java.sql.Types.SQLXML
                            // case java.sql.Types.ARRAY:
                            // case java.sql.Types.JAVA_OBJECT:
                            // case java.sql.Types.BLOB:
                            // case java.sql.Types.CLOB:
                            // case java.sql.Types.NCLOB:
                            // case java.sql.Types.DATALINK:
                            // case java.sql.Types.DISTINCT:
                            // case java.sql.Types.OTHER:
                            // case java.sql.Types.REF:
                            // case java.sql.Types.ROWID:
                            // case java.sql.Types.STRUC
                    }
                }
            }
        }
    }

    /**
     * Executes prepared statement and closes all resources of this instance.
     *
     * @throws IOException Thrown, if the input could not be closed properly.
     */
    @Override
    public void close() throws IOException {
        try {
            if (upload != null) {
                upload.executeBatch();
                upload.close();
            }
        } catch (SQLException se) {
            LOG.info("Inputformat couldn't be closed - " + se.getMessage());
        } finally {
            upload = null;
            batchCount = 0;
        }

        try {
            if (dbConn != null) {
                dbConn.close();
            }
        } catch (SQLException se) {
            LOG.info("Inputformat couldn't be closed - " + se.getMessage());
        } finally {
            dbConn = null;
        }
    }

    /**
     * get db all index
     *
     * @throws SQLException
     */
    public void getRealIndexes() throws SQLException {
        Map<String, List<String>> map = Maps.newHashMap();
        ResultSet rs;
        if (EDatabaseType.ORACLE.name().equalsIgnoreCase(dbType)) {
            PreparedStatement ps = dbConn.prepareStatement(String.format(GET_ORACLE_INDEX_SQL, tableName));
            rs = ps.executeQuery();
        } else {
            rs = dbConn.getMetaData().getIndexInfo(null, null, tableName, true, false);
        }

        while (rs.next()) {
            String indexName = rs.getString("INDEX_NAME");
            if (!map.containsKey(indexName)) {
                map.put(indexName, new ArrayList<>());
            }
            String column_name = rs.getString("COLUMN_NAME");
            if (StringUtils.isNotBlank(column_name)) {
                column_name = column_name.toUpperCase();
            }
            map.get(indexName).add(column_name);
        }

        for (Map.Entry<String, List<String>> entry : map.entrySet()) {
            String k = entry.getKey();
            List<String> v = entry.getValue();
            if (v != null && v.size() != 0 && v.get(0) != null) {
                realIndexes.put(k, v);
            }
        }
    }

    /**
     * get db all column name
     *
     * @throws SQLException
     */
    public void getFullColumns() throws SQLException {
        String schema = null;
        if (EDatabaseType.ORACLE.name().equalsIgnoreCase(dbType)) {
            String[] parts = tableName.split("\\.");
            if (parts.length == 2) {
                schema = parts[0].toUpperCase();
                tableName = parts[1];
            }
        }

        ResultSet rs = dbConn.getMetaData().getColumns(null, schema, tableName, null);
        while (rs.next()) {
            String columnName = rs.getString("COLUMN_NAME");
            if (StringUtils.isNotBlank(columnName)) {
                fullField.add(columnName.toUpperCase());
            }
        }

    }

    public static JDBCOutputFormatBuilder buildJDBCOutputFormat() {
        return new JDBCOutputFormatBuilder();
    }

    public static class JDBCOutputFormatBuilder {
        private final RetractJDBCOutputFormat format;

        protected JDBCOutputFormatBuilder() {
            this.format = new RetractJDBCOutputFormat();
        }

        public JDBCOutputFormatBuilder setUsername(String username) {
            format.username = username;
            return this;
        }

        public JDBCOutputFormatBuilder setPassword(String password) {
            format.password = password;
            return this;
        }

        public JDBCOutputFormatBuilder setDrivername(String drivername) {
            format.drivername = drivername;
            return this;
        }

        public JDBCOutputFormatBuilder setDBUrl(String dbURL) {
            format.dbURL = dbURL;
            return this;
        }

        public JDBCOutputFormatBuilder setInsertQuery(String query) {
            format.insertQuery = query;
            return this;
        }


        public JDBCOutputFormatBuilder setBatchInterval(int batchInterval) {
            format.batchInterval = batchInterval;
            return this;
        }

        public JDBCOutputFormatBuilder setSqlTypes(int[] typesArray) {
            format.typesArray = typesArray;
            return this;
        }

        public JDBCOutputFormatBuilder setTableName(String tableName) {
            format.tableName = tableName;
            return this;
        }

        public JDBCOutputFormatBuilder setDBSink(DBSink dbSink) {
            format.dbSink = dbSink;
            return this;
        }

        public JDBCOutputFormatBuilder setDBType(String dbType) {
            format.dbType = dbType;
            return this;
        }


        /**
         * Finalizes the configuration and checks validity.
         *
         * @return Configured RetractJDBCOutputFormat
         */
        public RetractJDBCOutputFormat finish() {
            if (format.username == null) {
                LOG.info("Username was not supplied separately.");
            }
            if (format.password == null) {
                LOG.info("Password was not supplied separately.");
            }
            if (format.dbURL == null) {
                throw new IllegalArgumentException("No dababase URL supplied.");
            }
            if (format.insertQuery == null) {
                throw new IllegalArgumentException("No insertQuery suplied");
            }
            if (format.drivername == null) {
                throw new IllegalArgumentException("No driver supplied");
            }
            return format;
        }
    }

}
