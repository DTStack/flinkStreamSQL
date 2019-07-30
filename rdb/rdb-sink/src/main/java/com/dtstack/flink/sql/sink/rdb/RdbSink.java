/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flink.sql.sink.rdb;

import com.dtstack.flink.sql.sink.IStreamSinkGener;
import com.dtstack.flink.sql.sink.rdb.format.RetractJDBCOutputFormat;
import com.dtstack.flink.sql.sink.rdb.table.RdbTableInfo;
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

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Reason:
 * Date: 2018/11/27
 * Company: www.dtstack.com
 *
 * @author maqi
 */
public abstract class RdbSink implements RetractStreamTableSink<Row>, Serializable, IStreamSinkGener<RdbSink> {

    protected String driverName;

    protected String dbURL;

    protected String userName;

    protected String password;

    protected String dbType;

    protected int batchNum = 1;

    protected long batchWaitInterval = 10000;

    protected int[] sqlTypes;

    protected String tableName;

    protected String sql;

    protected List<String> primaryKeys;

    protected String[] fieldNames;

    private TypeInformation[] fieldTypes;

    private int parallelism = -1;

    public RichSinkFunction createJdbcSinkFunc() {
        if (driverName == null || dbURL == null || userName == null
                || password == null || sqlTypes == null || tableName == null) {
            throw new RuntimeException("any of params in(driverName, dbURL, userName, password, type, tableName) " +
                    " must not be null. please check it!!!");
        }
        RetractJDBCOutputFormat outputFormat = getOutputFormat();
        outputFormat.setDbURL(dbURL);
        outputFormat.setDrivername(driverName);
        outputFormat.setUsername(userName);
        outputFormat.setPassword(password);
        outputFormat.setInsertQuery(sql);
        outputFormat.setBatchNum(batchNum);
        outputFormat.setBatchWaitInterval(batchWaitInterval);
        outputFormat.setTypesArray(sqlTypes);
        outputFormat.setTableName(tableName);
        outputFormat.setDbType(dbType);
        outputFormat.setDbSink(this);

        outputFormat.verifyField();
        OutputFormatSinkFunction outputFormatSinkFunc = new OutputFormatSinkFunction(outputFormat);
        return outputFormatSinkFunc;
    }


    @Override
    public RdbSink genStreamSink(TargetTableInfo targetTableInfo) {
        RdbTableInfo rdbTableInfo = (RdbTableInfo) targetTableInfo;

        String tmpDbURL = rdbTableInfo.getUrl();
        String tmpUserName = rdbTableInfo.getUserName();
        String tmpPassword = rdbTableInfo.getPassword();
        String tmpTableName = rdbTableInfo.getTableName();

        Integer tmpSqlBatchSize = rdbTableInfo.getBatchSize();
        if (tmpSqlBatchSize != null) {
            setBatchNum(tmpSqlBatchSize);
        }

        Long batchWaitInterval = rdbTableInfo.getBatchWaitInterval();
        if (batchWaitInterval != null) {
            setBatchWaitInterval(batchWaitInterval);
        }

        Integer tmpSinkParallelism = rdbTableInfo.getParallelism();
        if (tmpSinkParallelism != null) {
            setParallelism(tmpSinkParallelism);
        }

        List<String> fields = Arrays.asList(rdbTableInfo.getFields());
        List<Class> fieldTypeArray = Arrays.asList(rdbTableInfo.getFieldClasses());

        this.driverName = getDriverName();
        this.dbURL = tmpDbURL;
        this.userName = tmpUserName;
        this.password = tmpPassword;
        this.tableName = tmpTableName;
        this.primaryKeys = rdbTableInfo.getPrimaryKeys();
        this.dbType = rdbTableInfo.getType();

        buildSql(tableName, fields);
        buildSqlTypes(fieldTypeArray);
        return this;
    }


    /**
     * By now specified class type conversion.
     * FIXME Follow-up has added a new type of time needs to be modified
     *
     * @param fieldTypeArray
     */
    protected void buildSqlTypes(List<Class> fieldTypeArray) {

        int[] tmpFieldsType = new int[fieldTypeArray.size()];
        for (int i = 0; i < fieldTypeArray.size(); i++) {
            String fieldType = fieldTypeArray.get(i).getName();
            if (fieldType.equals(Integer.class.getName())) {
                tmpFieldsType[i] = Types.INTEGER;
            }else if (fieldType.equals(Boolean.class.getName())) {
                tmpFieldsType[i] = Types.BOOLEAN;
            }else if (fieldType.equals(Long.class.getName())) {
                tmpFieldsType[i] = Types.BIGINT;
            } else if (fieldType.equals(Byte.class.getName())) {
                tmpFieldsType[i] = Types.TINYINT;
            } else if (fieldType.equals(Short.class.getName())) {
                tmpFieldsType[i] = Types.SMALLINT;
            } else if (fieldType.equals(String.class.getName())) {
                tmpFieldsType[i] = Types.CHAR;
            } else if (fieldType.equals(Byte.class.getName())) {
                tmpFieldsType[i] = Types.BINARY;
            } else if (fieldType.equals(Float.class.getName())) {
                tmpFieldsType[i] = Types.FLOAT;
            } else if (fieldType.equals(Double.class.getName())) {
                tmpFieldsType[i] = Types.DOUBLE;
            } else if (fieldType.equals(Timestamp.class.getName())) {
                tmpFieldsType[i] = Types.TIMESTAMP;
            } else if (fieldType.equals(BigDecimal.class.getName())) {
                tmpFieldsType[i] = Types.DECIMAL;
            } else if (fieldType.equals(Date.class.getName())) {
                tmpFieldsType[i] = Types.DATE;
            } else {
                throw new RuntimeException("no support field type for sql. the input type:" + fieldType);
            }
        }

        this.sqlTypes = tmpFieldsType;
    }


    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        RichSinkFunction richSinkFunction = createJdbcSinkFunc();
        DataStreamSink streamSink = dataStream.addSink(richSinkFunction);
        streamSink.name(tableName);
        if (parallelism > 0) {
            streamSink.setParallelism(parallelism);
        }
    }

    @Override
    public TableSink<Tuple2<Boolean, Row>> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        return this;
    }


    public void setBatchNum(int batchNum) {
        this.batchNum = batchNum;
    }

    public void setBatchWaitInterval(long batchWaitInterval) {
        this.batchWaitInterval = batchWaitInterval;
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


    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    /**
     * you need to implements  this method in your own class.
     *
     * @param tableName
     * @param fields
     */
    public abstract void buildSql(String tableName, List<String> fields);

    /**
     * sqlserver and oracle maybe implement
     *
     * @param tableName
     * @param fieldNames
     * @param realIndexes
     * @return
     */
    public abstract String buildUpdateSql(String tableName, List<String> fieldNames, Map<String, List<String>> realIndexes, List<String> fullField);

    public abstract String getDriverName();

    public abstract RetractJDBCOutputFormat getOutputFormat();

}
