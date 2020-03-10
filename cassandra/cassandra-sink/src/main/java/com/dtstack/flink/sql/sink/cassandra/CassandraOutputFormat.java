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

package com.dtstack.flink.sql.sink.cassandra;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.dtstack.flink.sql.outputformat.DtRichOutputFormat;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;

/**
 * OutputFormat to write tuples into a database.
 * The OutputFormat has to be configured using the supplied OutputFormatBuilder.
 *
 * @see Tuple
 * @see DriverManager
 */
public class CassandraOutputFormat extends AbstractDtRichOutputFormat<Tuple2> {
    private static final long serialVersionUID = -7994311331389155692L;

    private static final Logger LOG = LoggerFactory.getLogger(CassandraOutputFormat.class);

    private String address;
    private String tableName;
    private String userName;
    private String password;
    private String database;
    private Integer maxRequestsPerConnection;
    private Integer coreConnectionsPerHost;
    private Integer maxConnectionsPerHost;
    private Integer maxQueueSize;
    private Integer readTimeoutMillis;
    private Integer connectTimeoutMillis;
    private Integer poolTimeoutMillis;

    protected String[] fieldNames;
    TypeInformation<?>[] fieldTypes;

    private Cluster cluster;
    private Session session = null;

    public CassandraOutputFormat() {
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
    public void open(int taskNumber, int numTasks) {
        initMetric();
        try {
            if (session == null) {
                QueryOptions queryOptions = new QueryOptions();
                //The default consistency level for queries: ConsistencyLevel.TWO.
                queryOptions.setConsistencyLevel(ConsistencyLevel.QUORUM);
                Integer maxRequestsPerConnection = this.maxRequestsPerConnection == null ? 1 : this.maxRequestsPerConnection;
                Integer coreConnectionsPerHost = this.coreConnectionsPerHost == null ? 8 : this.coreConnectionsPerHost;
                Integer maxConnectionsPerHost = this.maxConnectionsPerHost == null ? 32768 : this.maxConnectionsPerHost;
                Integer maxQueueSize = this.maxQueueSize == null ? 100000 : this.maxQueueSize;
                Integer readTimeoutMillis = this.readTimeoutMillis == null ? 60000 : this.readTimeoutMillis;
                Integer connectTimeoutMillis = this.connectTimeoutMillis == null ? 60000 : this.connectTimeoutMillis;
                Integer poolTimeoutMillis = this.poolTimeoutMillis == null ? 60000 : this.poolTimeoutMillis;
                Integer cassandraPort = 0;

                ArrayList serversList = new ArrayList();
                //Read timeout or connection timeout Settings
                SocketOptions so = new SocketOptions()
                        .setReadTimeoutMillis(readTimeoutMillis)
                        .setConnectTimeoutMillis(connectTimeoutMillis);

                //The cluster USES hostdistance.local in the same machine room
                //Hostdistance. REMOTE is used for different machine rooms
                //Ignore use HostDistance. IGNORED
                PoolingOptions poolingOptions = new PoolingOptions()
                        //Each connection allows a maximum of 64 concurrent requests
                        .setMaxRequestsPerConnection(HostDistance.LOCAL, maxRequestsPerConnection)
                        //Have at least two connections to each machine in the cluster
                        .setCoreConnectionsPerHost(HostDistance.LOCAL, coreConnectionsPerHost)
                        //There are up to eight connections to each machine in the cluster
                        .setMaxConnectionsPerHost(HostDistance.LOCAL, maxConnectionsPerHost)
                        .setMaxQueueSize(maxQueueSize)
                        .setPoolTimeoutMillis(poolTimeoutMillis);
                //重试策略
                RetryPolicy retryPolicy = DowngradingConsistencyRetryPolicy.INSTANCE;

                for (String server : StringUtils.split(address, ",")) {
                    cassandraPort = Integer.parseInt(StringUtils.split(server, ":")[1]);
                    serversList.add(InetAddress.getByName(StringUtils.split(server, ":")[0]));
                }

                if (userName == null || userName.isEmpty() || password == null || password.isEmpty()) {
                    cluster = Cluster.builder().addContactPoints(serversList).withRetryPolicy(retryPolicy)
                            .withPort(cassandraPort)
                            .withPoolingOptions(poolingOptions).withSocketOptions(so)
                            .withQueryOptions(queryOptions).build();
                } else {
                    cluster = Cluster.builder().addContactPoints(serversList).withRetryPolicy(retryPolicy)
                            .withPort(cassandraPort)
                            .withPoolingOptions(poolingOptions).withSocketOptions(so)
                            .withCredentials(userName, password)
                            .withQueryOptions(queryOptions).build();
                }
                // 建立连接 连接已存在的键空间
                session = cluster.connect(database);
                LOG.info("connect cassandra is successed!");
            }
        } catch (Exception e) {
            LOG.error("connect cassandra is error:" + e.getMessage());
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
        try {
            if (retract) {
                insertWrite(row);
            } else {
                //do nothing
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("writeRecord() failed", e);
        }
    }

    private void insertWrite(Row row) {
        try {

            if(outRecords.getCount() % ROW_PRINT_FREQUENCY == 0){
                LOG.info("Receive data : {}", row);
            }

            String cql = buildSql(row);
            if (cql != null) {
                ResultSet resultSet = session.execute(cql);
                resultSet.wasApplied();
                outRecords.inc();
            }
        } catch (Exception e) {
            if(outDirtyRecords.getCount() % DIRTY_PRINT_FREQUENCY == 0){
                LOG.error("record insert failed, total dirty num:{}, current record:{}", outDirtyRecords.getCount(), row.toString());
                LOG.error("", e);
            }

            outDirtyRecords.inc();
        }
    }

    private String buildSql(Row row) {
        StringBuffer fields = new StringBuffer();
        StringBuffer values = new StringBuffer();
        for (int index = 0; index < row.getArity(); index++) {
            if (row.getField(index) == null) {
            } else {
                fields.append(fieldNames[index] + ",");
                if (row.getField(index) instanceof String) {
                    values.append("'" + row.getField(index) + "'" + ",");
                } else {
                    values.append(row.getField(index) + ",");
                }
            }
        }
        fields.deleteCharAt(fields.length() - 1);
        values.deleteCharAt(values.length() - 1);
        String cql = "INSERT INTO " + database + "." + tableName + " (" + fields.toString() + ") "
                + " VALUES (" + values.toString() + ")";
        return cql;
    }

    /**
     * Executes prepared statement and closes all resources of this instance.
     *
     * @throws IOException Thrown, if the input could not be closed properly.
     */
    @Override
    public void close() throws IOException {
        try {
            if (session != null) {
                session.close();
            }
        } catch (Exception e) {
            LOG.error("Error while closing session.", e);
        }
        try {
            if (cluster != null) {
                cluster.close();
            }
        } catch (Exception e) {
            LOG.error("Error while closing cluster.", e);
        }
        LOG.info("close cassandra is successed!");
    }

    public static CassandraFormatBuilder buildOutputFormat() {
        return new CassandraFormatBuilder();
    }

    public static class CassandraFormatBuilder {
        private final CassandraOutputFormat format;

        protected CassandraFormatBuilder() {
            this.format = new CassandraOutputFormat();
        }

        public CassandraFormatBuilder setUsername(String username) {
            format.userName = username;
            return this;
        }

        public CassandraFormatBuilder setPassword(String password) {
            format.password = password;
            return this;
        }

        public CassandraFormatBuilder setAddress(String address) {
            format.address = address;
            return this;
        }

        public CassandraFormatBuilder setTableName(String tableName) {
            format.tableName = tableName;
            return this;
        }

        public CassandraFormatBuilder setDatabase(String database) {
            format.database = database;
            return this;
        }

        public CassandraFormatBuilder setFieldNames(String[] fieldNames) {
            format.fieldNames = fieldNames;
            return this;
        }

        public CassandraFormatBuilder setFieldTypes(TypeInformation<?>[] fieldTypes) {
            format.fieldTypes = fieldTypes;
            return this;
        }

        public CassandraFormatBuilder setMaxRequestsPerConnection(Integer maxRequestsPerConnection) {
            format.maxRequestsPerConnection = maxRequestsPerConnection;
            return this;
        }

        public CassandraFormatBuilder setCoreConnectionsPerHost(Integer coreConnectionsPerHost) {
            format.coreConnectionsPerHost = coreConnectionsPerHost;
            return this;
        }

        public CassandraFormatBuilder setMaxConnectionsPerHost(Integer maxConnectionsPerHost) {
            format.maxConnectionsPerHost = maxConnectionsPerHost;
            return this;
        }

        public CassandraFormatBuilder setMaxQueueSize(Integer maxQueueSize) {
            format.maxQueueSize = maxQueueSize;
            return this;
        }

        public CassandraFormatBuilder setReadTimeoutMillis(Integer readTimeoutMillis) {
            format.readTimeoutMillis = readTimeoutMillis;
            return this;
        }

        public CassandraFormatBuilder setConnectTimeoutMillis(Integer connectTimeoutMillis) {
            format.connectTimeoutMillis = connectTimeoutMillis;
            return this;
        }

        public CassandraFormatBuilder setPoolTimeoutMillis(Integer poolTimeoutMillis) {
            format.poolTimeoutMillis = poolTimeoutMillis;
            return this;
        }

        /**
         * Finalizes the configuration and checks validity.
         *
         * @return Configured RetractJDBCOutputFormat
         */
        public CassandraOutputFormat finish() {
            if (format.userName == null) {
                LOG.info("Username was not supplied separately.");
            }
            if (format.password == null) {
                LOG.info("Password was not supplied separately.");
            }
            if (format.address == null) {
                throw new IllegalArgumentException("No address URL supplied.");
            }
            if (format.database == null) {
                throw new IllegalArgumentException("No dababase suplied");
            }
            if (format.tableName == null) {
                throw new IllegalArgumentException("No tableName supplied");
            }
            return format;
        }
    }
}
