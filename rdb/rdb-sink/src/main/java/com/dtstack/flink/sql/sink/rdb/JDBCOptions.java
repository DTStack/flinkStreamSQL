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


import com.dtstack.flink.sql.core.rdb.JdbcCheckKeys;
import com.dtstack.flink.sql.sink.rdb.dialect.JDBCDialect;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;


public class JDBCOptions {

	private String dbUrl;
	private String tableName;
	private String driverName;
	private String username;
	private String password;
	private String schema;
    private JDBCDialect dialect;

    private JDBCOptions(String dbUrl, String tableName, String driverName, String username,
                        String password, String schema, JDBCDialect dialect) {
        this.dbUrl = dbUrl;
        this.tableName = tableName;
        this.driverName = driverName;
        this.username = username;
        this.password = password;
        this.schema = schema;
        this.dialect = dialect;
    }

	public String getDbUrl() {
		return dbUrl;
	}

	public String getTableName() {
		return tableName;
	}

	public String getDriverName() {
		return driverName;
	}

	public String getUsername() {
		return username;
	}

	public String getPassword() {
		return password;
	}

	public JDBCDialect getDialect() {
		return dialect;
	}

    public String getSchema() {
        return schema;
    }

    public Map<String, String> buildCheckProperties() {
		Map<String, String> properties = Maps.newHashMap();
		properties.put(JdbcCheckKeys.DRIVER_NAME, getDriverName());
		properties.put(JdbcCheckKeys.URL_KEY, getDbUrl());
		properties.put(JdbcCheckKeys.USER_NAME_KEY, getUsername());
		properties.put(JdbcCheckKeys.PASSWORD_KEY, getPassword());
		properties.put(JdbcCheckKeys.SCHEMA_KEY, getSchema());
		properties.put(JdbcCheckKeys.TABLE_NAME_KEY, getTableName());
		properties.put(JdbcCheckKeys.OPERATION_NAME_KEY, "jdbcOutputFormat");
		properties.put(JdbcCheckKeys.TABLE_TYPE_KEY, "sink");
		return properties;
	}

    public static Builder builder() {
		return new Builder();
	}

	@Override
    public boolean equals(Object o) {
        if (o instanceof JDBCOptions) {
            JDBCOptions options = (JDBCOptions) o;
            return Objects.equals(dbUrl, options.dbUrl) &&
                    Objects.equals(tableName, options.tableName) &&
                    Objects.equals(driverName, options.driverName) &&
                    Objects.equals(username, options.username) &&
                    Objects.equals(password, options.password) &&
                    Objects.equals(schema, options.schema) &&
                    Objects.equals(dialect.getClass().getName(), options.dialect.getClass().getName());
        } else {
            return false;
        }
    }

	/**
	 * Builder of {@link JDBCOptions}.
	 */
	public static class Builder {
		private String dbUrl;
		private String tableName;
		private String driverName;
		private String username;
		private String password;
        private String schema;
		private JDBCDialect dialect;

		/**
		 * required, table name.
		 */
		public Builder setTableName(String tableName) {
			this.tableName = tableName;
			return this;
		}

		/**
		 * optional, user name.
		 */
		public Builder setUsername(String username) {
			this.username = username;
			return this;
		}

		/**
		 * optional, password.
		 */
		public Builder setPassword(String password) {
			this.password = password;
			return this;
		}

		/**
		 * optional, driver name, dialect has a default driver name,
		 * See {@link JDBCDialect#defaultDriverName}.
		 */
		public Builder setDriverName(String driverName) {
            this.driverName = driverName;
            return this;
        }

        /**
         *  optional, schema info
         */
        public Builder setSchema(String schema) {
            this.schema = schema;
            return this;
        }

		/**
		 * required, JDBC DB url.
		 */
		public Builder setDbUrl(String dbUrl) {
			this.dbUrl = dbUrl;
			return this;
		}

		public Builder setDialect(JDBCDialect dialect) {
			this.dialect = dialect;
			return this;
		}

		public JDBCOptions build() {
			checkNotNull(dbUrl, "No dbURL supplied.");
			checkNotNull(tableName, "No tableName supplied.");

			if (this.driverName == null) {
				Optional<String> optional = dialect.defaultDriverName();
				this.driverName = optional.orElseGet(() -> {
					throw new NullPointerException("No driverName supplied.");
				});
			}

            return new JDBCOptions(dbUrl, tableName, driverName, username, password, schema, dialect);
		}
	}
}
