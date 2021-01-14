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
import com.dtstack.flink.sql.sink.rdb.format.JDBCUpsertOutputFormat;
import com.dtstack.flink.sql.sink.rdb.table.RdbTableInfo;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.table.AbstractTargetTableInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;
import com.dtstack.flink.sql.sink.rdb.dialect.JDBCDialect;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * Reason:
 * Date: 2018/11/27
 * Company: www.dtstack.com
 *
 * @author maqi
 */
public abstract class AbstractRdbSink implements RetractStreamTableSink<Row>, Serializable, IStreamSinkGener<AbstractRdbSink> {
    protected String name;

    protected String dbUrl;

    protected String userName;

    protected String password;

    protected String dbType;

    protected int batchNum = 100;

    protected long batchWaitInterval = 10000;

    protected int[] sqlTypes;

    protected String tableName;

    protected String registerTabName;

    protected String sql;

    protected List<String> primaryKeys;

    protected String[] fieldNames;

    private TypeInformation[] fieldTypes;

    private int parallelism = 1;

    protected String schema;

    protected JDBCDialect jdbcDialect;

    protected boolean allReplace;

    protected String updateMode;

    public List<String> fieldList;
    public List<String> fieldTypeList;
    public List<AbstractTableInfo.FieldExtraInfo> fieldExtraInfoList;

    public AbstractRdbSink(JDBCDialect jdbcDialect) {
        this.jdbcDialect = jdbcDialect;
    }

    @Override
    public AbstractRdbSink genStreamSink(AbstractTargetTableInfo targetTableInfo) {
        RdbTableInfo rdbTableInfo = (RdbTableInfo) targetTableInfo;
        this.name = rdbTableInfo.getName();
        this.batchNum = rdbTableInfo.getBatchSize() == null ? batchNum : rdbTableInfo.getBatchSize();
        this.batchWaitInterval = rdbTableInfo.getBatchWaitInterval() == null ?
                batchWaitInterval : rdbTableInfo.getBatchWaitInterval();
        this.parallelism = rdbTableInfo.getParallelism() == null ? parallelism : rdbTableInfo.getParallelism();
        this.dbUrl = rdbTableInfo.getUrl();
        this.userName = rdbTableInfo.getUserName();
        this.password = rdbTableInfo.getPassword();
        this.tableName = rdbTableInfo.getTableName();
        this.registerTabName = rdbTableInfo.getName();
        this.primaryKeys = rdbTableInfo.getPrimaryKeys();
        this.dbType = rdbTableInfo.getType();
        this.schema = rdbTableInfo.getSchema();
        List<Class> fieldTypeArray = Arrays.asList(rdbTableInfo.getFieldClasses());
        this.sqlTypes = JDBCTypeConvertUtils.buildSqlTypes(fieldTypeArray);
        this.allReplace = rdbTableInfo.isAllReplace();
        this.updateMode = rdbTableInfo.getUpdateMode();
        this.fieldList = rdbTableInfo.getFieldList();
        this.fieldTypeList = rdbTableInfo.getFieldTypeList();
        this.fieldExtraInfoList = rdbTableInfo.getFieldExtraInfoList();
        return this;
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        DataStreamSink dataStreamSink = dataStream.addSink(new OutputFormatSinkFunction(getOutputFormat()))
                .setParallelism(parallelism)
                .name(registerTabName);
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

    public abstract JDBCUpsertOutputFormat getOutputFormat();

}
