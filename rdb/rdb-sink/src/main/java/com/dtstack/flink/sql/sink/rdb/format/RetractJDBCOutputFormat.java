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

import com.dtstack.flink.sql.sink.rdb.RdbSink;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.dtstack.flink.sql.sink.MetricOutputFormat;

/**
 * OutputFormat to write tuples into a database.
 * The OutputFormat has to be configured using the supplied OutputFormatBuilder.
 *
 */
public class RetractJDBCOutputFormat extends MetricOutputFormat {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RetractJDBCOutputFormat.class);

    private String username;
    private String password;
    private String drivername;
    private String dbURL;
    private String tableName;
    private String dbType;
    private RdbSink dbSink;
    // trigger preparedStatement execute batch interval
    private long batchWaitInterval = 10000l;
    // PreparedStatement execute batch num
    private int batchNum = 1;
    private String insertQuery;
    public int[] typesArray;

    private Connection dbConn;
    private PreparedStatement upload;
    private AtomicInteger batchCount = new AtomicInteger(0);
    private transient ScheduledThreadPoolExecutor timerService;


    //index field
    private Map<String, List<String>> realIndexes = Maps.newHashMap();
    //full field
    private List<String> fullField = Lists.newArrayList();

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
            LOG.info("PreparedStatement execute batch num is {}", batchNum);
            establishConnection();
            initMetric();

            if (batchWaitInterval > 0) {
                LOG.info("open batch wait interval scheduled, interval is {} ms", batchWaitInterval);

                timerService = new ScheduledThreadPoolExecutor(1);
                timerService.scheduleAtFixedRate(() -> {
                    submitExecuteBatch();
                }, 0, batchWaitInterval, TimeUnit.MILLISECONDS);

            }

            if (dbConn.getMetaData().getTables(null, null, tableName, null).next()) {
                if (isReplaceInsertQuery()) {
                    insertQuery = dbSink.buildUpdateSql(tableName, Arrays.asList(dbSink.getFieldNames()), realIndexes, fullField);
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
        batchCount.incrementAndGet();
        if (batchCount.get() >= batchNum) {
            submitExecuteBatch();
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


    private synchronized void submitExecuteBatch() {
        try {
            this.upload.executeBatch();
            this.batchCount.set(0);
        } catch (SQLException e) {
            LOG.error("", e);
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
            if (null != timerService) {
                timerService.shutdown();
                LOG.info("batch wait interval scheduled service  closed ");
            }
        } catch (SQLException se) {
            LOG.info("Inputformat couldn't be closed - ", se);
        } finally {
            upload = null;
            batchCount.set(0);
        }

        try {
            if (dbConn != null) {
                dbConn.close();
            }
        } catch (SQLException se) {
            LOG.info("Inputformat couldn't be closed - ", se);
        } finally {
            dbConn = null;
        }
    }


    public boolean isReplaceInsertQuery() throws SQLException {
        return false;
    }

    public void verifyField() {
        if (StringUtils.isBlank(username)) {
            LOG.info("Username was not supplied separately.");
        }
        if (StringUtils.isBlank(password)) {
            LOG.info("Password was not supplied separately.");
        }
        if (StringUtils.isBlank(dbURL)) {
            throw new IllegalArgumentException("No dababase URL supplied.");
        }
        if (StringUtils.isBlank(insertQuery)) {
            throw new IllegalArgumentException("No insertQuery suplied");
        }
        if (StringUtils.isBlank(drivername)) {
            throw new IllegalArgumentException("No driver supplied");
        }
    }


    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setDrivername(String drivername) {
        this.drivername = drivername;
    }

    public void setDbURL(String dbURL) {
        this.dbURL = dbURL;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    public void setDbSink(RdbSink dbSink) {
        this.dbSink = dbSink;
    }

    public void setBatchNum(int batchNum) {
        this.batchNum = batchNum;
    }

    public void setInsertQuery(String insertQuery) {
        this.insertQuery = insertQuery;
    }

    public void setTypesArray(int[] typesArray) {
        this.typesArray = typesArray;
    }

    public String getDbType() {
        return dbType;
    }

    public RdbSink getDbSink() {
        return dbSink;
    }

    public Connection getDbConn() {
        return dbConn;
    }

    public String getTableName() {
        return tableName;
    }

    public Map<String, List<String>> getRealIndexes() {
        return realIndexes;
    }


    public void setBatchWaitInterval(long batchWaitInterval) {
        this.batchWaitInterval = batchWaitInterval;
    }

    public List<String> getFullField() {
        return fullField;
    }
}
