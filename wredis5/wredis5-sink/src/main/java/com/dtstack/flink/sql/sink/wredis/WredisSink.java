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

package com.dtstack.flink.sql.sink.wredis;

import com.dtstack.flink.sql.sink.IStreamSinkGener;
import com.dtstack.flink.sql.sink.wredis.table.WredisTableInfo;
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

public class WredisSink implements RetractStreamTableSink<Row>, IStreamSinkGener<WredisSink> {

    protected String[] fieldNames;

    protected TypeInformation<?>[] fieldTypes;

    protected String url;

    protected String key;

    protected String tableName;

    protected String password;

    protected List<String> primaryKeys;

    protected int timeout;
    protected int parallelism;
    public WredisSink(){

    }

    @Override
    public WredisSink genStreamSink(TargetTableInfo targetTableInfo) {
        WredisTableInfo redisTableInfo = (WredisTableInfo) targetTableInfo;
        this.url = redisTableInfo.getUrl();
        this.key = redisTableInfo.getKey();
        this.password = redisTableInfo.getPassword();
        this.tableName = redisTableInfo.getTableName();
        this.primaryKeys = redisTableInfo.getPrimaryKeys();
        this.timeout = redisTableInfo.getTimeout();
        this.parallelism = targetTableInfo.getParallelism();
        return this;
    }

    @Override
    public TypeInformation<Row> getRecordType() {
        return new RowTypeInfo(fieldTypes, fieldNames);
    }

    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        WredisOutputFormat.RedisOutputFormatBuilder builder = WredisOutputFormat.buildRedisOutputFormat();
        builder.setUrl(this.url)
                .setKey(this.key)
                .setTableName(this.tableName)
                .setPassword(this.password)
                .setFieldNames(this.fieldNames)
                .setFieldTypes(this.fieldTypes)
                .setPrimaryKeys(this.primaryKeys)
                .setTimeout(this.timeout);
        WredisOutputFormat redisOutputFormat = builder.finish();
        RichSinkFunction richSinkFunction = new OutputFormatSinkFunction(redisOutputFormat);
        DataStreamSink dataStreamSink = dataStream.addSink(richSinkFunction);
        dataStreamSink.name(tableName);
        dataStreamSink.setParallelism(parallelism);
    }

    @Override
    public TupleTypeInfo<Tuple2<Boolean, Row>> getOutputType() {
        return new TupleTypeInfo(org.apache.flink.table.api.Types.BOOLEAN(), getRecordType());
    }

    @Override
    public String[] getFieldNames() {
        return fieldNames;
    }

    @Override
    public TypeInformation<?>[] getFieldTypes() {
        return fieldTypes;
    }

    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        return this;
    }
}
