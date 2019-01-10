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

import com.dtstack.flink.sql.sink.IStreamSinkGener;
import com.dtstack.flink.sql.sink.hbase.table.HbaseTableInfo;
import com.dtstack.flink.sql.table.TargetTableInfo;
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

import java.util.List;

/**
 * Date: 2018/09/14
 * Company: www.dtstack.com
 * @author sishu.yss
 */
public class HbaseSink implements RetractStreamTableSink<Row>, IStreamSinkGener<HbaseSink> {

    protected String[] fieldNames;
    TypeInformation<?>[] fieldTypes;
    protected String zookeeperQuorum;
    protected String port;
    protected String parent;
    protected String tableName;
    protected String[] rowkey;
    protected List<String> primaryKeys;
    protected Integer parallelism;
    protected boolean ignoreRowKeyColumn = false;

    public HbaseSink() {
        // TO DO NOTHING
    }

    @Override
    public HbaseSink genStreamSink(TargetTableInfo targetTableInfo) {
        HbaseTableInfo hbaseTableInfo = (HbaseTableInfo) targetTableInfo;
        this.zookeeperQuorum = hbaseTableInfo.getHost();
        this.port = hbaseTableInfo.getPort();
        this.parent = hbaseTableInfo.getParent();
        this.tableName = hbaseTableInfo.getTableName();
        this.rowkey = hbaseTableInfo.getRowkey();
        this.ignoreRowKeyColumn = hbaseTableInfo.getIgnoreRowKeyColumn();
        this.primaryKeys = targetTableInfo.getPrimaryKeys();
        this.parallelism = hbaseTableInfo.getParallelism();
        return this;
    }

    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        HbaseOutputFormat.HbaseOutputFormatBuilder builder = HbaseOutputFormat.buildHbaseOutputFormat();
        builder.setHost(this.zookeeperQuorum)
                .setZkParent(this.parent)
                .setTable(this.tableName)
                .setRowkey(rowkey)
                .setColumnNames(fieldNames)
                .ignoreRowKeyColumn(ignoreRowKeyColumn)
                .setPrimaryKeys(primaryKeys);
        HbaseOutputFormat outputFormat = builder.finish();
        RichSinkFunction richSinkFunction = new OutputFormatSinkFunction(outputFormat);
        DataStreamSink dataStreamSink = dataStream.addSink(richSinkFunction);
        dataStreamSink.name(tableName);
        if (parallelism!=null && parallelism > 0) {
            dataStreamSink.setParallelism(parallelism);
        }
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
