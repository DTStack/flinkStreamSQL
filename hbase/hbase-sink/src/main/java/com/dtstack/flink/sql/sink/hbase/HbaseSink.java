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


package com.dtstack.flink.sql.sink.hbase;

import com.dtstack.flink.sql.dirtyManager.manager.DirtyDataManager;
import com.dtstack.flink.sql.sink.IStreamSinkGener;
import com.dtstack.flink.sql.sink.hbase.table.HbaseTableInfo;
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

import java.util.Map;
import java.util.Properties;

/**
 * Date: 2018/09/14
 * Company: www.dtstack.com
 *
 * @author sishu.yss
 */
public class HbaseSink implements RetractStreamTableSink<Row>, IStreamSinkGener<HbaseSink> {

    protected String[] fieldNames;
    protected Map<String, String> columnNameFamily;
    protected String zookeeperQuorum;
    protected String port;
    protected String parent;
    protected String tableName;
    protected String rowkey;
    protected String registerTabName;
    protected boolean kerberosAuthEnable;
    protected String regionserverKeytabFile;
    protected String regionserverPrincipal;
    protected String securityKrb5Conf;
    protected String zookeeperSaslClient;
    protected String batchSize;
    protected String batchWaitInterval;
    TypeInformation<?>[] fieldTypes;
    private String clientPrincipal;
    private String clientKeytabFile;
    private int parallelism = 1;

    private Properties dirtyProperties;


    public HbaseSink() {
        // TO DO NOTHING
    }

    @Override
    public HbaseSink genStreamSink(AbstractTargetTableInfo targetTableInfo) {
        HbaseTableInfo hbaseTableInfo = (HbaseTableInfo) targetTableInfo;
        this.zookeeperQuorum = hbaseTableInfo.getHost();
        this.port = hbaseTableInfo.getPort();
        this.parent = hbaseTableInfo.getParent();
        this.tableName = hbaseTableInfo.getTableName();
        this.rowkey = hbaseTableInfo.getRowkey();
        this.columnNameFamily = hbaseTableInfo.getColumnNameFamily();
        this.registerTabName = hbaseTableInfo.getName();

        this.kerberosAuthEnable = hbaseTableInfo.isKerberosAuthEnable();
        this.regionserverKeytabFile = hbaseTableInfo.getRegionserverKeytabFile();
        this.regionserverPrincipal = hbaseTableInfo.getRegionserverPrincipal();
        this.securityKrb5Conf = hbaseTableInfo.getSecurityKrb5Conf();
        this.zookeeperSaslClient = hbaseTableInfo.getZookeeperSaslClient();

        this.clientKeytabFile = hbaseTableInfo.getClientKeytabFile();
        this.clientPrincipal = hbaseTableInfo.getClientPrincipal();

        this.dirtyProperties = hbaseTableInfo.getDirtyProperties();

        this.batchSize = hbaseTableInfo.getBatchSize();
        this.batchWaitInterval = hbaseTableInfo.getBatchWaitInterval();

        Integer tmpSinkParallelism = hbaseTableInfo.getParallelism();
        if (tmpSinkParallelism != null) {
            this.parallelism = tmpSinkParallelism;
        }
        return this;
    }

    @Override
    public DataStreamSink<Tuple2<Boolean, Row>> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        HbaseOutputFormat.HbaseOutputFormatBuilder builder = HbaseOutputFormat.buildHbaseOutputFormat();
        HbaseOutputFormat outputFormat = builder
                .setHost(this.zookeeperQuorum)
                .setZkParent(this.parent)
                .setTable(this.tableName)
                .setRowkey(rowkey)
                .setColumnNames(fieldNames)
                .setColumnNameFamily(columnNameFamily)
                .setKerberosAuthEnable(kerberosAuthEnable)
                .setRegionserverKeytabFile(regionserverKeytabFile)
                .setRegionserverPrincipal(regionserverPrincipal)
                .setSecurityKrb5Conf(securityKrb5Conf)
                .setZookeeperSaslClient(zookeeperSaslClient)
                .setClientPrincipal(clientPrincipal)
                .setClientKeytabFile(clientKeytabFile)
                .setBatchSize(Integer.parseInt(batchSize))
                .setBatchWaitInterval(Long.parseLong(batchWaitInterval))
                .setDirtyManager(DirtyDataManager.newInstance(dirtyProperties))
                .finish();

        RichSinkFunction richSinkFunction = new OutputFormatSinkFunction(outputFormat);
        DataStreamSink dataStreamSink = dataStream.addSink(richSinkFunction).name(registerTabName);

        if (parallelism > 0) {
            dataStreamSink.setParallelism(parallelism);
        }

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
