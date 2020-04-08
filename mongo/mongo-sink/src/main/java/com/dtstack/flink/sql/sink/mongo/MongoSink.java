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


package com.dtstack.flink.sql.sink.mongo;

import com.dtstack.flink.sql.sink.IStreamSinkGener;
import com.dtstack.flink.sql.sink.mongo.table.MongoTableInfo;
import com.dtstack.flink.sql.table.TargetTableInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;

/**
 * Reason:
 * Date: 2018/11/6
 *
 * @author xuqianjin
 */
public class MongoSink implements RetractStreamTableSink<Row>, IStreamSinkGener<MongoSink> {

    protected String[] fieldNames;
    TypeInformation<?>[] fieldTypes;
    protected String address;
    protected String tableName;
    protected String userName;
    protected String password;
    protected String database;

    public MongoSink() {
        // TO DO NOTHING
    }

    @Override
    public MongoSink genStreamSink(TargetTableInfo targetTableInfo) {
        MongoTableInfo mongoTableInfo = (MongoTableInfo) targetTableInfo;
        this.address = mongoTableInfo.getAddress();
        this.tableName = mongoTableInfo.getTableName();
        this.userName = mongoTableInfo.getUserName();
        this.password = mongoTableInfo.getPassword();
        this.database = mongoTableInfo.getDatabase();
        return this;
    }

    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        MongoOutputFormat.MongoOutputFormatBuilder builder = MongoOutputFormat.buildMongoOutputFormat();
        builder.setAddress(this.address)
                .setDatabase(this.database)
                .setTableName(this.tableName)
                .setPassword(this.password)
                .setUsername(this.userName)
                .setFieldNames(this.fieldNames)
                .setFieldTypes(this.fieldTypes);

        MongoOutputFormat outputFormat = builder.finish();
        RichSinkFunction richSinkFunction = new OutputFormatSinkFunction(outputFormat);
        dataStream.addSink(richSinkFunction);
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
