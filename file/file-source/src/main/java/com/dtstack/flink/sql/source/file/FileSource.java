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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
    private final AtomicBoolean running = new AtomicBoolean(true);

    private URI fileUri;

    private InputStream inputStream;

    private String charset;

    private final AtomicInteger count = new AtomicInteger(0);

    private final AtomicInteger errorCount = new AtomicInteger(0);

    @Override
    public Table genStreamSource(AbstractSourceTableInfo sourceTableInfo,
                                 StreamExecutionEnvironment env,
                                 StreamTableEnvironment tableEnv) {
        FileSource fileSource = new FileSource();
        FileSourceTableInfo tableInfo = (FileSourceTableInfo) sourceTableInfo;

        DataStreamSource<?> source = fileSource.initDataStream(tableInfo, env);
        String fields = StringUtils.join(tableInfo.getFields(), ",");

        return tableEnv.fromDataStream(source, fields);
    }

    public DataStreamSource<?> initDataStream(FileSourceTableInfo tableInfo,
                                              StreamExecutionEnvironment env) {
        deserializationSchema = tableInfo.getDeserializationSchema();
        fileUri = URI.create(tableInfo.getFilePath() + SP + tableInfo.getFileName());
        charset = tableInfo.getCharsetName();
        return env.addSource(
            this,
            tableInfo.getOperatorName(),
            tableInfo.buildRowTypeInfo());
    }

    /**
     * 根据存储位置的不同，获取不同的input stream
     *
     * @param fileUri file uri
     * @return input stream
     */
    private InputStream getInputStream(URI fileUri) {
        try {
            String scheme = fileUri.getScheme() == null ? "local" : fileUri.getScheme();
            switch (scheme.toLowerCase(Locale.ROOT)) {
                case "local":
                    return fromLocalFile(fileUri);
                case "hdfs":
                    return fromHdfsFile(fileUri);
                default:
                    throw new UnsupportedOperationException(
                        String.format("Unsupported type [%s] of file.", scheme)
                    );
            }
        } catch (IOException e) {
            throw new SuppressRestartsException(e);
        }
    }

    /**
     * 从HDFS上获取文件内容
     *
     * @param fileUri file uri of file
     * @return hdfs file input stream
     * @throws IOException reader exception
     */
    private InputStream fromHdfsFile(URI fileUri) throws IOException {
        Configuration conf = new Configuration();

        // get conf from HADOOP_CONF_DIR
        String hadoopConfDir = System.getenv("HADOOP_CONF_DIR");
        String confHome = hadoopConfDir == null ? "." : hadoopConfDir;

        conf.addResource(new Path(confHome + SP + "hdfs-site.xml"));

        FileSystem fs = FileSystem.get(fileUri, conf);
        return fs.open(new Path(fileUri.getPath()));
    }

    /**
     * 从本地获取文件内容
     *
     * @param fileUri file uri of file
     * @return local file input stream
     * @throws FileNotFoundException read exception
     */
    private InputStream fromLocalFile(URI fileUri) throws FileNotFoundException {
        File file = new File(fileUri.getPath());
        if (file.exists()) {
            return new FileInputStream(file);
        } else {
            throw new SuppressRestartsException(new IOException(
                String.format("File not exist. File path: [%s]", fileUri.getPath())));
        }
    }

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
        inputStream = getInputStream(fileUri);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, charset));

        while (running.get()) {
            String line = bufferedReader.readLine();
            if (line == null) {
                running.compareAndSet(true, false);
                inputStream.close();
                break;
            } else {
                try {
                    Row row = deserializationSchema.deserialize(line.getBytes());
                    if (row == null) {
                        throw new IOException("Deserialized row is null");
                    }
                    ctx.collect(row);
                    count.incrementAndGet();
                } catch (IOException e) {
                    errorCount.incrementAndGet();
                }
            }
        }
        printCount();
    }

    @Override
    public void cancel() {
        LOG.info("File source cancel..");
        running.compareAndSet(true, false);
        if (inputStream != null) {
            try {
                inputStream.close();
            } catch (IOException ioException) {
                LOG.error("File input stream close error!");
            }
        }
        printCount();
    }

    private void printCount() {
        LOG.info("Read count: {}", count.get());
        LOG.info("Error count: {}", errorCount.get());
    }
}
