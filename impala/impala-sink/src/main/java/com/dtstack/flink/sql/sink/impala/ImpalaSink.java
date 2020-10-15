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

package com.dtstack.flink.sql.sink.impala;

import com.dtstack.flink.sql.sink.IStreamSinkGener;
import com.dtstack.flink.sql.sink.impala.table.ImpalaTableInfo;
import com.dtstack.flink.sql.table.AbstractTableInfo;
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

import java.util.List;
import java.util.Objects;

/**
 * Date: 2020/10/14
 * Company: www.dtstack.com
 *
 * @author tiezhu
 */
public class ImpalaSink implements RetractStreamTableSink<Row>, IStreamSinkGener<ImpalaSink> {

    private static final String DEFAULT_STORE_TYPE = "kudu";
    private static final String DEFAULT_PARTITION_MODE = "dynamic";

    protected String[] fieldNames;
    TypeInformation<?>[] fieldTypes;
    protected String dbUrl;
    protected String userName;
    protected String password;
    protected Integer authMech;

    protected String keytabPath;
    protected String krb5confPath;
    protected String principal;

    protected int batchSize = 100;
    protected long batchWaitInterval = 10000;
    protected String tableName;
    protected String registerTabName;
    protected String storeType;

    protected List<String> primaryKeys;
    private int parallelism = 1;
    protected String schema;
    protected String updateMode;
    protected Boolean enablePartition;
    public List<String> fieldList;
    public List<String> fieldTypeList;
    public List<AbstractTableInfo.FieldExtraInfo> fieldExtraInfoList;
    protected String partitionFields;

    public ImpalaSink() {
        // do Nothing
    }

    @Override
    public ImpalaSink genStreamSink(AbstractTargetTableInfo targetTableInfo) {
        ImpalaTableInfo impalaTableInfo = (ImpalaTableInfo) targetTableInfo;
        this.dbUrl = getImpalaJdbcUrl(impalaTableInfo);
        this.password = impalaTableInfo.getPassword();
        this.userName = impalaTableInfo.getUserName();
        this.authMech = impalaTableInfo.getAuthMech();

        this.principal = impalaTableInfo.getPrincipal();
        this.keytabPath = impalaTableInfo.getKeyTabFilePath();
        this.krb5confPath = impalaTableInfo.getKrb5FilePath();

        this.updateMode = Objects.isNull(impalaTableInfo.getUpdateMode()) ?
                "append" : impalaTableInfo.getUpdateMode();

        this.batchSize = Objects.isNull(impalaTableInfo.getBatchSize()) ?
                batchSize : impalaTableInfo.getBatchSize();
        this.batchWaitInterval = Objects.isNull(impalaTableInfo.getBatchWaitInterval()) ?
                batchWaitInterval : impalaTableInfo.getBatchWaitInterval();
        this.parallelism = Objects.isNull(impalaTableInfo.getParallelism()) ?
                parallelism : impalaTableInfo.getParallelism();
        this.registerTabName = impalaTableInfo.getTableName();

        this.fieldList = impalaTableInfo.getFieldList();
        this.fieldTypeList = impalaTableInfo.getFieldTypeList();
        this.fieldExtraInfoList = impalaTableInfo.getFieldExtraInfoList();
        this.tableName = impalaTableInfo.getTableName();
        this.schema = impalaTableInfo.getSchema();
        this.primaryKeys = impalaTableInfo.getPrimaryKeys();
        this.partitionFields = impalaTableInfo.getPartitionFields();

        this.storeType = Objects.isNull(impalaTableInfo.getStoreType()) ?
                DEFAULT_STORE_TYPE : impalaTableInfo.getStoreType();
        this.enablePartition = impalaTableInfo.isEnablePartition();

        return this;
    }

    /**
     * build Impala Jdbc Url according to authMech
     *
     * @param impalaTableInfo impala table info
     * @return jdbc url with auth mech info
     */
    public String getImpalaJdbcUrl(ImpalaTableInfo impalaTableInfo) {
        Integer authMech = impalaTableInfo.getAuthMech();
        String newUrl = impalaTableInfo.getUrl();
        StringBuilder urlBuffer = new StringBuilder(impalaTableInfo.getUrl());
        if (authMech == EAuthMech.NoAuthentication.getType()) {
            return newUrl;
        } else if (authMech == EAuthMech.Kerberos.getType()) {
            String krbRealm = impalaTableInfo.getKrbRealm();
            String krbHostFqdn = impalaTableInfo.getKrbHostFQDN();
            String krbServiceName = impalaTableInfo.getKrbServiceName();
            urlBuffer.append(";"
                    .concat("AuthMech=1;")
                    .concat("KrbRealm=").concat(krbRealm).concat(";")
                    .concat("KrbHostFQDN=").concat(krbHostFqdn).concat(";")
                    .concat("KrbServiceName=").concat(krbServiceName).concat(";")
            );
            newUrl = urlBuffer.toString();
        } else if (authMech == EAuthMech.UserName.getType()) {
            urlBuffer.append(";"
                    .concat("AuthMech=3;")
                    .concat("UID=").concat(impalaTableInfo.getUserName()).concat(";")
                    .concat("PWD=;")
                    .concat("UseSasl=0")
            );
            newUrl = urlBuffer.toString();
        } else if (authMech == EAuthMech.NameANDPassword.getType()) {
            urlBuffer.append(";"
                    .concat("AuthMech=3;")
                    .concat("UID=").concat(impalaTableInfo.getUserName()).concat(";")
                    .concat("PWD=").concat(impalaTableInfo.getPassword())
            );
            newUrl = urlBuffer.toString();
        } else {
            throw new IllegalArgumentException("The value of authMech is illegal, Please select 0, 1, 2, 3");
        }
        return newUrl;
    }

    private ImpalaOutputFormat buildImpalaOutputFormat() {

        return ImpalaOutputFormat.getImpalaBuilder()
                .setDbUrl(dbUrl)
                .setPassword(password)
                .setUserName(userName)
                .setSchema(schema)
                .setTableName(tableName)
                .setUpdateMode(updateMode)
                .setBatchSize(batchSize)
                .setBatchWaitInterval(batchWaitInterval)
                .setPrimaryKeys(primaryKeys)
                .setPartitionFields(partitionFields)
                .setFieldList(fieldList)
                .setFieldTypeList(fieldTypeList)
                .setFieldExtraInfoList(fieldExtraInfoList)
                .setStoreType(storeType)
                .setEnablePartition(enablePartition)
                .setUpdateMode(updateMode)
                .setAuthMech(authMech)
                .setKeyTabPath(keytabPath)
                .setKrb5ConfPath(krb5confPath)
                .setPrincipal(principal)
                .build();
    }

    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        consumeDataStream(dataStream);
    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        ImpalaOutputFormat outputFormat = buildImpalaOutputFormat();
        RichSinkFunction richSinkFunction = new OutputFormatSinkFunction(outputFormat);
        DataStreamSink dataStreamSink = dataStream.addSink(richSinkFunction)
                .setParallelism(parallelism)
                .name(tableName);
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
        return new TupleTypeInfo<>(org.apache.flink.table.api.Types.BOOLEAN(), getRecordType());
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
