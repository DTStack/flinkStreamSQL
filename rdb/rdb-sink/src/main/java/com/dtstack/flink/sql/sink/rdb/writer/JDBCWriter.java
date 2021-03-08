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

import com.dtstack.flink.sql.core.rdb.util.JdbcConnectionUtil;
import com.dtstack.flink.sql.exception.ExceptionTrace;
import com.dtstack.flink.sql.outputformat.AbstractDtRichOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.apache.flink.types.Row;
import org.slf4j.Logger;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * JDBCWriter used to execute statements (e.g. INSERT, UPSERT, DELETE).
 */
public interface JDBCWriter extends Serializable {

	int DIRTYDATA_PRINT_FREQUENTY = 1000;

	void initMetricOutput(AbstractDtRichOutputFormat metricOutputFormat);

	/**
	 * Open the writer by JDBC Connection.
	 */
	void open(Connection connection) throws SQLException;

	/**
	 *  Create Statement from Connection. Used where the connection is created
	 * @throws SQLException
	 */
	void prepareStatement(Connection connection) throws SQLException;

	/**
	 * Add record to writer, the writer may cache the data.
	 */
	void addRecord(Tuple2<Boolean, Row> record);

	/**
	 * Submits a batch of commands to the database for execution.
	 */
	void executeBatch(Connection connection) throws SQLException;

	/**
	 *   clean batch cache when addBatch error
	 *   @throws SQLException
	 */
	void cleanBatchWhenError() throws SQLException;

	/**
	 *  Submits a single of commands to the database for execution.
	 */
	void executeUpdate(Connection connection) throws SQLException;

	/**
	 * Close JDBC related statements and other classes.
	 */
	void close() throws SQLException;

	default void dealExecuteError(Connection connection,
								  Exception e,
								  AbstractDtRichOutputFormat metricOutputFormat,
								  Row row,
								  long errorLimit,
								  Logger LOG) {
		JdbcConnectionUtil.rollBack(connection);
		JdbcConnectionUtil.commit(connection);

		if (metricOutputFormat.outDirtyRecords.getCount() % DIRTYDATA_PRINT_FREQUENTY == 0 ||
			LOG.isDebugEnabled()) {
			LOG.error(
				String.format(
					"record insert failed. \nRow: [%s]. \nCause: [%s]",
					row.toString(),
					ExceptionTrace.traceOriginalCause(e)));
		}

		if (errorLimit > -1) {
			if (metricOutputFormat.outDirtyRecords.getCount() > errorLimit) {
				throw new SuppressRestartsException(
					new Throwable(
						String.format("dirty data Count: [%s]. Error cause: [%s]",
							metricOutputFormat.outDirtyRecords.getCount(),
							ExceptionTrace.traceOriginalCause(e)))
				);
			}
		}

		metricOutputFormat.outDirtyRecords.inc();
	}
}
