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

import com.dtstack.flink.sql.outputformat.DtRichOutputFormat;
import com.dtstack.flink.sql.util.JDBCUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * OutputFormat to write Rows into a JDBC database.
 *
 * @see Row
 * @see DriverManager
 */
public abstract class AbstractJDBCOutputFormat<T> extends DtRichOutputFormat<T> {

	private static final long serialVersionUID = 1L;
    public static final int DEFAULT_FLUSH_MAX_SIZE = 100;
    public static final long DEFAULT_FLUSH_INTERVAL_MILLS = 10000L;
    public static final boolean DEFAULT_ALLREPLACE_VALUE = false;

	private static final Logger LOG = LoggerFactory.getLogger(AbstractJDBCOutputFormat.class);

	protected final String username;
	protected final String password;
	private final String drivername;
	protected final String dbURL;

	protected transient Connection connection;

	public AbstractJDBCOutputFormat(String username, String password, String drivername, String dbURL) {
		this.username = username;
		this.password = password;
		this.drivername = drivername;
		this.dbURL = dbURL;
	}

	@Override
	public void configure(Configuration parameters) {
	}

	protected void establishConnection() throws SQLException, ClassNotFoundException, IOException {
		JDBCUtils.forName(drivername, getClass().getClassLoader());
		if (username == null) {
			connection = DriverManager.getConnection(dbURL);
		} else {
			connection = DriverManager.getConnection(dbURL, username, password);
		}
		connection.setAutoCommit(false);
	}

	protected void closeDbConnection() throws IOException {
		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException se) {
				LOG.warn("JDBC connection could not be closed: " + se.getMessage());
			} finally {
				connection = null;
			}
		}
	}
}
