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

 

package com.dtstack.flink.sql.sink.mysql;

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

import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;

/**
 * Date: 2017/2/27
 * Company: www.dtstack.com
 * @author xuchao
 */

public abstract class DBSink implements RetractStreamTableSink<Row> {

    protected String driverName;

    protected String dbURL;

    protected String userName;

    protected String password;

    protected int batchInterval = 1;

    protected int[] sqlTypes;

    protected String tableName;

    protected String sql;

    protected List<String> primaryKeys;

    protected String[] fieldNames;

    private TypeInformation[] fieldTypes;

    private int parallelism = -1;

    public RichSinkFunction createJdbcSinkFunc(){

        if(driverName == null || dbURL == null || userName == null
                || password == null || sqlTypes == null || tableName == null){
            throw new RuntimeException("any of params in(driverName, dbURL, userName, password, type, tableName) " +
                    " must not be null. please check it!!!");
        }

        RetractJDBCOutputFormat.JDBCOutputFormatBuilder jdbcFormatBuild = RetractJDBCOutputFormat.buildJDBCOutputFormat();
        jdbcFormatBuild.setDBUrl(dbURL);
        jdbcFormatBuild.setDrivername(driverName);
        jdbcFormatBuild.setUsername(userName);
        jdbcFormatBuild.setPassword(password);
        jdbcFormatBuild.setInsertQuery(sql);
        jdbcFormatBuild.setBatchInterval(batchInterval);
        jdbcFormatBuild.setSqlTypes(sqlTypes);
        jdbcFormatBuild.setTableName(tableName);
        RetractJDBCOutputFormat outputFormat = jdbcFormatBuild.finish();

        OutputFormatSinkFunction outputFormatSinkFunc = new OutputFormatSinkFunction(outputFormat);
        return outputFormatSinkFunc;
    }

    /**
     * By now specified class type conversion.
     * FIXME Follow-up has added a new type of time needs to be modified
     * @param fieldTypeArray
     */
    protected void buildSqlTypes(List<Class> fieldTypeArray){

        int[] tmpFieldsType = new int[fieldTypeArray.size()];
        for(int i=0; i<fieldTypeArray.size(); i++){
            String fieldType = fieldTypeArray.get(i).getName();
            if(fieldType.equals(Integer.class.getName())){
                tmpFieldsType[i] = Types.INTEGER;
            }else if(fieldType.equals(Long.class.getName())){
                tmpFieldsType[i] = Types.BIGINT;
            }else if(fieldType.equals(Byte.class.getName())){
                tmpFieldsType[i] = Types.TINYINT;
            }else if(fieldType.equals(Short.class.getName())){
                tmpFieldsType[i] = Types.SMALLINT;
            }else if(fieldType.equals(String.class.getName())){
                tmpFieldsType[i] = Types.CHAR;
            }else if(fieldType.equals(Byte.class.getName())){
                tmpFieldsType[i] = Types.BINARY;
            }else if(fieldType.equals(Float.class.getName()) || fieldType.equals(Double.class.getName())){
                tmpFieldsType[i] = Types.DOUBLE;
            }else if (fieldType.equals(Timestamp.class.getName())){
                tmpFieldsType[i] = Types.TIMESTAMP;
            }else{
                throw new RuntimeException("no support field type for sql. the input type:" + fieldType);
            }
        }

        this.sqlTypes = tmpFieldsType;
    }

    /**
     * Set the default frequency submit updated every submission
     * @param batchInterval
     */
    public void setBatchInterval(int batchInterval) {
        this.batchInterval = batchInterval;
    }

    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        RichSinkFunction richSinkFunction = createJdbcSinkFunc();
        DataStreamSink streamSink = dataStream.addSink(richSinkFunction);
        streamSink.name(tableName);
        if(parallelism > 0){
            streamSink.setParallelism(parallelism);
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


    public void setParallelism(int parallelism){
        this.parallelism = parallelism;
    }

    public void buildSql(String tableName, List<String> fields){
        throw new RuntimeException("you need to overwrite this method in your own class.");
    }
}
