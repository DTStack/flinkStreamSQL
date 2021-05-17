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

package com.dtstack.flink.sql.sink.solr;

import com.dtstack.flink.sql.factory.DTThreadFactory;
import com.dtstack.flink.sql.outputformat.AbstractDtRichOutputFormat;
import com.dtstack.flink.sql.sink.solr.client.CloudSolrClientProvider;
import com.dtstack.flink.sql.sink.solr.options.KerberosOptions;
import com.dtstack.flink.sql.sink.solr.options.SolrClientOptions;
import com.dtstack.flink.sql.sink.solr.options.SolrWriteOptions;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.security.krb5.KrbException;

import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.security.PrivilegedActionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author wuren
 * @program flinkStreamSQL
 * @create 2021/05/17
 */
public class SolrOutputFormat extends AbstractDtRichOutputFormat<Tuple2<Boolean, Row>> {

    private static final Logger LOG = LoggerFactory.getLogger(SolrOutputFormat.class);

    //    private transient SolrClient solrClient;
    private final SolrClientOptions solrClientOptions;
    private final SolrWriteOptions solrWriteOptions;
    private final KerberosOptions kerberosOptions;
    private transient AtomicInteger rowCount;
    protected String[] fieldNames;
    private transient ScheduledExecutorService scheduler;
    private transient ScheduledFuture<?> scheduledFuture;
    CloudSolrClientProvider provider;

    public SolrOutputFormat(
            SolrClientOptions solrClientOptions,
            SolrWriteOptions solrWriteOptions,
            KerberosOptions kerberosOptions,
            String[] fieldNames) {
        this.solrClientOptions = solrClientOptions;
        this.solrWriteOptions = solrWriteOptions;
        this.kerberosOptions = kerberosOptions;
        this.fieldNames = fieldNames;
    }

    @Override
    public void configure(Configuration parameters) {}

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        provider = new CloudSolrClientProvider(solrClientOptions, kerberosOptions);
        try {
            provider.getClient();
        } catch (KrbException | LoginException e) {
            LOG.error("", e);
        }
        initMetric();
        initIntervalFlush();
        rowCount = new AtomicInteger(0);
    }

    @Override
    public void writeRecord(Tuple2<Boolean, Row> record) throws IOException {
        Row row = record.getField(1);
        SolrInputDocument solrDocument = new SolrInputDocument();
        int columnIndex = 0;
        for (String name : fieldNames) {
            solrDocument.setField(name, row.getField(columnIndex));
            columnIndex++;
        }
        try {
            provider.add(solrDocument);
        } catch (SolrServerException | PrivilegedActionException e) {
            LOG.error("", e);
        }
        if (rowCount.incrementAndGet() >= solrWriteOptions.getBufferFlushMaxRows()) {
            flush();
        }
    }

    @Override
    public void close() throws IOException {
        flush();
        try {
            provider.close();
        } catch (PrivilegedActionException e) {
            LOG.error("", e);
        }
    }

    private void initIntervalFlush() {
        if (solrWriteOptions.getBufferFlushIntervalMillis() != 0
                && solrWriteOptions.getBufferFlushMaxRows() != 1) {
            this.scheduler =
                    new ScheduledThreadPoolExecutor(1, new DTThreadFactory("solr-batch-flusher"));
            this.scheduledFuture =
                    this.scheduler.scheduleWithFixedDelay(
                            () -> {
                                flush();
                            },
                            solrWriteOptions.getBufferFlushIntervalMillis(),
                            solrWriteOptions.getBufferFlushIntervalMillis(),
                            TimeUnit.MILLISECONDS);
        }
    }

    private synchronized void flush() {
        try {
            provider.commit();
            rowCount.set(0);
        } catch (SolrServerException | IOException | PrivilegedActionException e) {
            LOG.error("", e);
        }
    }
}
