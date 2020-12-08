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

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AppendObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.dtstack.flink.sql.outputformat.AbstractDtRichOutputFormat;
import com.dtstack.flink.sql.sink.aws.util.AwsManager;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/**
 * @author tiezhu
 * date 2020/12/1
 * company dtstack
 */
public class AwsOutputFormat extends AbstractDtRichOutputFormat<Tuple2> {

    private static final Logger LOG = LoggerFactory.getLogger(AwsOutputFormat.class);

    private static final String LINE_BREAK = "\n";

    private transient AmazonS3Client client;

    private String accessKey;
    private String secretKey;
    private String bucket;
    private String bucketAcl;
    private String objectName;
    private String hostname;
    private InputStream inputStream;
    private Long position = 0L;

    private AwsOutputFormat() {
    }

    @Override
    public void configure(Configuration parameters) {
        LOG.warn("--- configure client ---");
        client = AwsManager.initClient(accessKey, secretKey, hostname);
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        LOG.warn("--- open ---");
        initMetric();
        position = AwsManager.getObjectPosition(bucket, objectName, client);
    }

    @Override
    public void writeRecord(Tuple2 record) throws IOException {
        String recordStr = record.f1.toString() + LINE_BREAK;
        int length = recordStr.getBytes().length;
        inputStream = new ByteArrayInputStream(recordStr.getBytes());
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(length);
        // 追加流式写入，但是这种情况下，可能会出现oom【因为数据都是缓存在内存中】
        AppendObjectRequest appendObjectRequest = new AppendObjectRequest(
                bucket, objectName, inputStream, metadata)
                .withPosition(position);

        client.appendObject(appendObjectRequest);
        position += length;
        outRecords.inc();
    }

    @Override
    public void close() throws IOException {
        if (Objects.nonNull(inputStream)) {
            inputStream.close();
        }

        if (Objects.nonNull(client)) {
            client.shutdown();
        }
    }

    public static AwsOutputFormatBuilder buildS3OutputFormat() {
        return new AwsOutputFormatBuilder();
    }

    public static class AwsOutputFormatBuilder {
        private final AwsOutputFormat awsOutputFormat;

        private AwsOutputFormatBuilder() {
            awsOutputFormat = new AwsOutputFormat();
        }

        public AwsOutputFormatBuilder setAccessKey(String accessKey) {
            awsOutputFormat.accessKey = accessKey;
            return this;
        }

        public AwsOutputFormatBuilder setSecretKey(String secretKey) {
            awsOutputFormat.secretKey = secretKey;
            return this;
        }

        public AwsOutputFormatBuilder setBucket(String bucket) {
            awsOutputFormat.bucket = bucket;
            return this;
        }

        public AwsOutputFormatBuilder setBucketAcl(String bucketAcl) {
            awsOutputFormat.bucketAcl = bucketAcl;
            return this;
        }

        public AwsOutputFormatBuilder setHostname(String hostname) {
            awsOutputFormat.hostname = hostname;
            return this;
        }

        public AwsOutputFormatBuilder setObjectName(String objectName) {
            awsOutputFormat.objectName = objectName;
            return this;
        }

        public AwsOutputFormat finish() {
            return awsOutputFormat;
        }
    }
}
