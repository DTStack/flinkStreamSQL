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
import com.dtstack.flink.sql.sink.rdb.RdbSink;
import com.dtstack.flink.sql.sink.rdb.format.RetractJDBCOutputFormat;
import com.dtstack.flink.sql.table.TargetTableInfo;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Date: 2019/11/11
 * Company: www.dtstack.com
 *
 * @author xiuzhu
 */

public class ImpalaSink extends RdbSink implements IStreamSinkGener<RdbSink> {

    private static final String IMPALA_DRIVER = "com.cloudera.impala.jdbc41.Driver";

    private ImpalaTableInfo impalaTableInfo;

    public ImpalaSink() {
    }

    @Override
    public RichSinkFunction createJdbcSinkFunc() {
        ImpalaOutputFormat outputFormat = (ImpalaOutputFormat) getOutputFormat();
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
        outputFormat.setSchema(impalaTableInfo.getSchema());
        outputFormat.setDbSink(this);
        outputFormat.setImpalaTableInfo(impalaTableInfo);

        outputFormat.verifyField();
        OutputFormatSinkFunction outputFormatSinkFunc = new OutputFormatSinkFunction(outputFormat);
        return outputFormatSinkFunc;
    }

    @Override
    public RdbSink genStreamSink(TargetTableInfo targetTableInfo) {
        ImpalaTableInfo impalaTableInfo = (ImpalaTableInfo) targetTableInfo;
        this.impalaTableInfo = impalaTableInfo;
        super.genStreamSink(targetTableInfo);
        return this;
    }

    @Override
    public void buildSql(String schema, String tableName, List<String> fields) {
        buildInsertSql(tableName, fields);
    }

    public void buildInsertSql(String tableName, List<String> fields) {

        String sqlTmp = "insert into " + tableName + " (${fields}) " + "${partition}"+ " values (${placeholder})";
        int fieldsSize = fields.size();
        boolean enablePartition = impalaTableInfo.isEnablePartition();
        if (enablePartition) {
            String partitionFieldsStr = impalaTableInfo.getPartitionFields();
            partitionFieldsStr = !StringUtils.isEmpty(partitionFieldsStr) ? partitionFieldsStr.replaceAll("\"", "'") : partitionFieldsStr;

            List<String> partitionFields = Arrays.asList(partitionFieldsStr.split(","));
            List<String> newFields = new ArrayList<>();
            for (String field : fields) {
                if (partitionFields.contains(field)){
                    continue;
                }
                newFields.add(field);
            }
            fields = newFields;
            String partition = String.format("partition(%s)", partitionFieldsStr);
            sqlTmp = sqlTmp.replace("${partition}", partition);
        } else {
            sqlTmp = sqlTmp.replace("${partition}", "");
        }

        String fieldsStr = "";
        String placeholder = "";
        for (int i= 0; i < fieldsSize; i++) {
            placeholder += ",?";
        }
        for (String fieldName : fields) {
            fieldsStr += "," + fieldName;
        }
        fieldsStr = fieldsStr.replaceFirst(",", "");
        placeholder = placeholder.replaceFirst(",", "");
        sqlTmp = sqlTmp.replace("${fields}", fieldsStr).replace("${placeholder}", placeholder);

        this.sql = sqlTmp;
    }

    @Override
    public String buildUpdateSql(String schema, String tableName, List<String> fieldNames, Map<String, List<String>> realIndexes, List<String> fullField) {
        return null;
    }

    @Override
    public String getDriverName() {
        return IMPALA_DRIVER;
    }

    @Override
    public RetractJDBCOutputFormat getOutputFormat() {
        return new ImpalaOutputFormat();
    }


    public ImpalaTableInfo getImpalaTableInfo() {
        return impalaTableInfo;
    }

    public void setImpalaTableInfo(ImpalaTableInfo impalaTableInfo) {
        this.impalaTableInfo = impalaTableInfo;
    }
}
