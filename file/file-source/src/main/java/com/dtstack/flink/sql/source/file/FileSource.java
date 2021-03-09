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

package com.dtstack.flink.sql.source.file;

import com.dtstack.flink.sql.source.IStreamSourceGener;
import com.dtstack.flink.sql.source.file.table.FileSourceTableInfo;
import com.dtstack.flink.sql.table.AbstractSourceTableInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.execution.SuppressRestartsException;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Locale;
import java.util.Objects;

/**
 * @author tiezhu
 * @date 2021/3/9 星期二
 * Company dtstack
 */
public class FileSource implements IStreamSourceGener<Table>, SourceFunction<Row> {

    private static final Logger LOG = LoggerFactory.getLogger(FileSource.class);

    private static final String SP = File.separator;

    private DeserializationSchema<Row> deserializationSchema;

    /**
     * Flag to mark the main work loop as alive.
     */
    private volatile boolean running = true;

    private FileSourceTableInfo fileSourceTableInfo;

    private InputStream inputStream;

    public void setDeserializationSchema(DeserializationSchema<Row> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
    }

    public void setFileSourceTableInfo(FileSourceTableInfo fileSourceTableInfo) {
        this.fileSourceTableInfo = fileSourceTableInfo;
    }

    public FileSourceTableInfo getFileSourceTableInfo() {
        return fileSourceTableInfo;
    }

    @Override
    public Table genStreamSource(AbstractSourceTableInfo sourceTableInfo,
                                 StreamExecutionEnvironment env,
                                 StreamTableEnvironment tableEnv) {
        FileSource fileSource = new FileSource();
        FileSourceTableInfo tableInfo = (FileSourceTableInfo) sourceTableInfo;
        fileSource.setFileSourceTableInfo(tableInfo);
        fileSource.setDeserializationSchema(tableInfo.buildDeserializationSchema());

        TypeInformation<Row> rowTypeInformation = tableInfo.getRowTypeInformation();
        String operatorName = tableInfo.getOperatorName();

        DataStreamSource<?> source = env.addSource(fileSource, operatorName, rowTypeInformation);

        String fields = StringUtils.join(fileSource.getFileSourceTableInfo().getFields(), ",");
        return tableEnv.fromDataStream(source, fields);
    }

    /**
     * 根据存储位置的不同，获取不同的input stream
     *
     * @param sourceTableInfo source table info
     * @return input stream
     * @throws Exception reader exception
     */
    private InputStream getInputStream(FileSourceTableInfo sourceTableInfo) throws Exception {
        switch (sourceTableInfo.getLocation().toLowerCase(Locale.ROOT)) {
            case "local":
                return fromLocalFile(sourceTableInfo);
            case "hdfs":
                return fromHdfsFile(sourceTableInfo);
            default:
                throw new IllegalArgumentException();
        }
    }

    /**
     * 从HDFS上获取文件内容
     *
     * @param sourceTableInfo source table info
     * @return hdfs file input stream
     * @throws Exception reader exception
     */
    private InputStream fromHdfsFile(FileSourceTableInfo sourceTableInfo) throws Exception {
        String filePath = sourceTableInfo.getFilePath();
        String fileName = sourceTableInfo.getFileName();
        String path = filePath + SP + fileName;

        Configuration conf = new Configuration();
        conf.addResource(new Path(sourceTableInfo.getHdfsSite()));
        conf.addResource(new Path(sourceTableInfo.getCoreSite()));
        FileSystem fs = FileSystem.newInstance(new URI(filePath), conf, sourceTableInfo.getHdfsUser());
        return fs.open(new Path(path));
    }

    /**
     * 从本地获取文件内容
     *
     * @param sourceTableInfo source table
     * @return local file input stream
     * @throws Exception read exception
     */
    private InputStream fromLocalFile(FileSourceTableInfo sourceTableInfo) throws Exception {
        String filePath = sourceTableInfo.getFilePath();
        String fileName = sourceTableInfo.getFileName();
        String path = filePath + SP + fileName;
        File file = new File(path);
        if (file.exists()) {
            return new FileInputStream(file);
        } else {
            throw new SuppressRestartsException(new IOException(
                String.format(
                    "File [%s] not exist. File path: [%s]",
                    sourceTableInfo.getFileName(),
                    sourceTableInfo.getFilePath())
            ));
        }
    }


    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
        inputStream = getInputStream(fileSourceTableInfo);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

        while (running) {
            String line = bufferedReader.readLine();
            if (line == null) {
                running = false;
                inputStream.close();
                break;
            } else {
                Row row = deserializationSchema.deserialize(line.getBytes());
                ctx.collect(row);
            }
        }
    }

    @Override
    public void cancel() {
        LOG.info("File source cancel..");
        running = false;
        if (Objects.nonNull(inputStream)) {
            try {
                inputStream.close();
            } catch (IOException ioException) {
                LOG.error("File input stream close error!");
            }
        }
    }
}
