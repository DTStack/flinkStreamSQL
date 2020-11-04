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


import com.dtstack.flink.sql.sink.IStreamSinkGener;
import com.dtstack.flink.sql.sink.cassandra.table.CassandraTableInfo;
import com.dtstack.flink.sql.table.AbstractTargetTableInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

/**
 * Reason:
 * Date: 2018/11/22
 *
 * @author xuqianjin
 */
public class CassandraSink implements RetractStreamTableSink<Row>, IStreamSinkGener<CassandraSink> {


    protected String[] fieldNames;
    TypeInformation<?>[] fieldTypes;
    protected String address;
    protected String tableName;
    protected String userName;
    protected String password;
    protected String database;
    protected Integer maxRequestsPerConnection;
    protected Integer coreConnectionsPerHost;
    protected Integer maxConnectionsPerHost;
    protected Integer maxQueueSize;
    protected Integer readTimeoutMillis;
    protected Integer connectTimeoutMillis;
    protected Integer poolTimeoutMillis;
    protected Integer parallelism = 1;
    protected String registerTableName;

    public CassandraSink() {
        // TO DO NOTHING
    }

    @Override
    public CassandraSink genStreamSink(AbstractTargetTableInfo targetTableInfo) {
        CassandraTableInfo cassandraTableInfo = (CassandraTableInfo) targetTableInfo;
        this.address = cassandraTableInfo.getAddress();
        this.tableName = cassandraTableInfo.getTableName();
        this.userName = cassandraTableInfo.getUserName();
        this.password = cassandraTableInfo.getPassword();
        this.database = cassandraTableInfo.getDatabase();
        this.maxRequestsPerConnection = cassandraTableInfo.getMaxRequestsPerConnection();
        this.coreConnectionsPerHost = cassandraTableInfo.getCoreConnectionsPerHost();
        this.maxConnectionsPerHost = cassandraTableInfo.getMaxConnectionsPerHost();
        this.maxQueueSize = cassandraTableInfo.getMaxQueueSize();
        this.readTimeoutMillis = cassandraTableInfo.getReadTimeoutMillis();
        this.connectTimeoutMillis = cassandraTableInfo.getConnectTimeoutMillis();
        this.poolTimeoutMillis = cassandraTableInfo.getPoolTimeoutMillis();
        this.parallelism = cassandraTableInfo.getParallelism();
        this.registerTableName = cassandraTableInfo.getTableName();
        return this;
    }

    @Override
    public DataStreamSink<Tuple2<Boolean, Row>> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        CassandraOutputFormat.CassandraFormatBuilder builder = CassandraOutputFormat.buildOutputFormat();
        builder.setAddress(this.address)
                .setDatabase(this.database)
                .setTableName(this.tableName)
                .setPassword(this.password)
                .setUsername(this.userName)
                .setMaxRequestsPerConnection(this.maxRequestsPerConnection)
                .setCoreConnectionsPerHost(this.coreConnectionsPerHost)
                .setMaxConnectionsPerHost(this.maxConnectionsPerHost)
                .setMaxQueueSize(this.maxQueueSize)
                .setReadTimeoutMillis(this.readTimeoutMillis)
                .setConnectTimeoutMillis(this.connectTimeoutMillis)
                .setPoolTimeoutMillis(this.poolTimeoutMillis)
                .setFieldNames(this.fieldNames)
                .setFieldTypes(this.fieldTypes);

        CassandraOutputFormat outputFormat = builder.finish();
        RichSinkFunction richSinkFunction = new OutputFormatSinkFunction(outputFormat);
        DataStreamSink dataStreamSink = dataStream.addSink(richSinkFunction)
                .setParallelism(parallelism)
                .name(registerTableName);
        return dataStreamSink;
    }

    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        return this;
    }

    @Override
    public TupleTypeInfo<Tuple2<Boolean, Row>> getOutputType() {
        return new TupleTypeInfo(org.apache.flink.table.api.Types.BOOLEAN(), getRecordType());
    }

    @Override
    public TypeInformation<Row> getRecordType() {
        return new RowTypeInfo(fieldTypes, fieldNames);
    }

    @Override
    public String[] getFieldNames() {
        return fieldNames;
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return fieldTypes;
    }

}
