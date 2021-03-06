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

import com.dtstack.flink.sql.exception.ExceptionTrace;
import com.dtstack.flink.sql.metric.MetricConstant;
import com.dtstack.flink.sql.source.IStreamSourceGener;
import com.dtstack.flink.sql.source.file.table.FileSourceTableInfo;
import com.dtstack.flink.sql.table.AbstractSourceTableInfo;
import com.dtstack.flink.sql.util.ThreadUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.metrics.Counter;
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
public class FileSource extends AbstractRichFunction implements IStreamSourceGener<Table>, SourceFunction<Row> {
    private static final Logger LOG = LoggerFactory.getLogger(FileSource.class);

    private static final Long METRIC_WAIT_TIME = 20L;

    private static final String SP = File.separator;

    private DeserializationSchema<Row> deserializationSchema;

    /**
     * Flag to mark the main work loop as alive.
     */
    private final AtomicBoolean running = new AtomicBoolean(true);

    private URI fileUri;

    private InputStream inputStream;

    private BufferedReader bufferedReader;

    private String charset;

    private int fromLine;

    protected transient Counter errorCounter;

    /**
     * tps ransactions Per Second
     */
    protected transient Counter numInRecord;

    /**
     * rps Record Per Second: deserialize data and out record num
     */
    protected transient Counter numInResolveRecord;

    @Override
    public Table genStreamSource(AbstractSourceTableInfo sourceTableInfo,
                                 StreamExecutionEnvironment env,
                                 StreamTableEnvironment tableEnv) {
        FileSource fileSource = new FileSource();
        FileSourceTableInfo tableInfo = (FileSourceTableInfo) sourceTableInfo;

        fileSource.initSource(tableInfo);

        String fields = StringUtils.join(tableInfo.getFields(), ",");

        DataStreamSource<Row> source = env.addSource(fileSource, tableInfo.getOperatorName(), tableInfo.getTypeInformation());

        return tableEnv.fromDataStream(source, fields);
    }

    /**
     * 初始化指标
     */
    public void initMetric() {
        RuntimeContext runtimeContext = getRuntimeContext();

        errorCounter = runtimeContext.getMetricGroup().counter(MetricConstant.DT_DIRTY_DATA_COUNTER);
        numInRecord = runtimeContext.getMetricGroup().counter(MetricConstant.DT_NUM_RECORDS_IN_COUNTER);
        numInResolveRecord = runtimeContext.getMetricGroup().counter(MetricConstant.DT_NUM_RECORDS_RESOVED_IN_COUNTER);
    }

    public void initSource(FileSourceTableInfo tableInfo) {
        deserializationSchema = tableInfo.getDeserializationSchema();
        fileUri = URI.create(tableInfo.getFilePath() + SP + tableInfo.getFileName());

        charset = tableInfo.getCharsetName();
        LOG.info("File charset: " + charset);

        fromLine = tableInfo.getFromLine();
        LOG.info("Read from line: " + fromLine);
    }

    /**
     * 根据存储位置的不同，获取不同的input stream
     *
     * @param fileUri file uri
     * @return input stream
     */
    private InputStream getInputStream(URI fileUri) {
        try {
            String scheme = fileUri.getScheme() == null ? FileSourceConstant.FILE_LOCAL : fileUri.getScheme();
            switch (scheme.toLowerCase(Locale.ROOT)) {
                case FileSourceConstant.FILE_LOCAL:
                    return fromLocalFile(fileUri);
                case FileSourceConstant.FILE_HDFS:
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
        initMetric();
        AtomicInteger currentLine = new AtomicInteger(0);
        String line = "";
        inputStream = getInputStream(fileUri);
        bufferedReader = new BufferedReader(new InputStreamReader(inputStream, charset));

        try {
            while (running.get()) {
                try {
                    line = bufferedReader.readLine();
                    if (line == null) {
                        running.compareAndSet(true, false);
                        inputStream.close();
                        bufferedReader.close();
                        break;
                    } else {
                        if (currentLine.incrementAndGet() < fromLine) {
                            continue;
                        }

                        numInRecord.inc();
                        Row row = deserializationSchema.deserialize(line.getBytes());
                        if (row == null) {
                            throw new IOException("Deserialized row is null");
                        }
                        ctx.collect(row);
                        numInResolveRecord.inc();
                    }
                } catch (IOException e) {
                    if (errorCounter.getCount() % 1000 == 0) {
                        LOG.error("Deserialize error! Record: " + line);
                        LOG.error("Cause: " + ExceptionTrace.traceOriginalCause(e));
                    }
                    errorCounter.inc();
                }
            }
        } finally {
            ThreadUtil.sleepSeconds(METRIC_WAIT_TIME);
        }
    }

    @Override
    public void cancel() {
        LOG.info("File source cancel..");
        running.compareAndSet(true, false);
        if (inputStream != null) {
            try {
                inputStream.close();
            } catch (IOException ioException) {
                LOG.error("File input stream close error!", ioException);
            }
        }

        if (bufferedReader != null) {
            try {
                bufferedReader.close();
            } catch (IOException ioException) {
                LOG.error("File buffer reader close error!", ioException);
            }
        }
    }
}
