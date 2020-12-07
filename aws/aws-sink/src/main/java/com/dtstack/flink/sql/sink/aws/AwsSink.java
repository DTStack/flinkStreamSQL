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

package com.dtstack.flink.sql.sink.aws;

import com.dtstack.flink.sql.sink.IStreamSinkGener;
import com.dtstack.flink.sql.sink.aws.table.AwsTableInfo;
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

import java.util.Objects;

/**
 * @author tiezhu
 * date 2020/12/1
 * company dtstack
 */
public class AwsSink implements RetractStreamTableSink<Row>, IStreamSinkGener<AwsSink> {

    protected String[] fieldNames;
    TypeInformation<?>[] fieldTypes;
    protected String accessKey;
    protected String secretKey;
    protected String bucket;
    protected String bucketAcl;
    protected String storageType;
    protected String hostname;
    protected String objectName;
    protected String registerName;

    protected int parallelism = 1;

    public AwsSink() {
    }

    @Override
    public AwsSink genStreamSink(AbstractTargetTableInfo targetTableInfo) {
        AwsTableInfo tableInfo = (AwsTableInfo) targetTableInfo;
        this.accessKey = tableInfo.getAccessKey();
        this.secretKey = tableInfo.getSecretKey();
        this.bucket = tableInfo.getBucketName();
        this.bucketAcl = tableInfo.getBucketAcl();
        this.hostname = tableInfo.getHostname();
        this.storageType = tableInfo.getStorageType();
        this.objectName = tableInfo.getObjectName();

        this.fieldNames = tableInfo.getFields();
        Integer s3TableInfoParallelism = tableInfo.getParallelism();
        if (Objects.nonNull(s3TableInfoParallelism)) {
            this.parallelism = s3TableInfoParallelism;
        }
        this.registerName = tableInfo.getName();

        return this;
    }

    @Override
    public void emitDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {

    }

    @Override
    public DataStreamSink<?> consumeDataStream(DataStream<Tuple2<Boolean, Row>> dataStream) {
        AwsOutputFormat.AwsOutputFormatBuilder builder = AwsOutputFormat.buildS3OutputFormat();
        AwsOutputFormat awsOutputFormat = builder.setAccessKey(accessKey)
                .setBucket(bucket)
                .setBucketAcl(bucketAcl)
                .setHostname(hostname)
                .setObjectName(objectName)
                .setSecretKey(secretKey)
                .finish();
        RichSinkFunction outputFormatSinkFunction = new OutputFormatSinkFunction(awsOutputFormat);
        DataStreamSink dataStreamSink = dataStream.addSink(outputFormatSinkFunction).name(registerName);
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
        return new TupleTypeInfo<>(org.apache.flink.table.api.Types.BOOLEAN(), getRecordType());
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
    public TypeInformation<Row> getRecordType() {
        return new RowTypeInfo(fieldTypes, fieldNames);
    }
}
