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

package com.dtstack.flink.sql.sink.http;

import com.dtstack.flink.sql.outputformat.AbstractDtRichOutputFormat;
import com.dtstack.flink.sql.sink.http.table.HttpTableInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author: chuixue
 * @create: 2021-03-03 10:41
 * @description:
 **/
public class HttpOutputFormat extends AbstractDtRichOutputFormat<Tuple2> {
    private static final Logger LOG = LoggerFactory.getLogger(HttpOutputFormat.class);

    private ObjectMapper mapper = new ObjectMapper();
    /**
     * Default connection timeout when connecting to the server socket (infinite).
     */
    private HttpTableInfo httpTableInfo;

    private transient Map<String, Object> sendMessage;

    private HttpOutputFormat() {
    }

    public static HttpOutputFormatBuilder buildHttpOutputFormat() {
        return new HttpOutputFormatBuilder();
    }

    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public void open(int taskNumber, int numTasks) {
        initMetric();
        sendMessage = new HashMap<>();
    }

    @Override
    public void writeRecord(Tuple2 record) {
        String value = null;
        CloseableHttpClient client = null;
        try {
            client = HttpClients.createDefault();
            RequestConfig requestConfig = RequestConfig.custom()
                    .setSocketTimeout(10000)
                    .setConnectTimeout(10000)
                    .setConnectionRequestTimeout(10000)
                    .build();
            HttpPost post = new HttpPost(this.httpTableInfo.getUrl());
            post.setConfig(requestConfig);
            // read data
            Tuple2<Boolean, Row> tupleTrans = record;
            if (!tupleTrans.f0) {
                return;
            }
            // not empty ,need send flag、tablename、fieldinfo、value.
            if (StringUtils.isNotEmpty(httpTableInfo.getFlag())) {
                sendMessage.put("flag", httpTableInfo.getFlag());
                sendMessage.put("tableName", httpTableInfo.getName());
            }
            // add field
            String[] fields = httpTableInfo.getFields();
            Row row = tupleTrans.getField(1);
            for (int i = 0; i < fields.length; i++) {
                sendMessage.put(fields[i], row.getField(i));
            }
            // send data
            value = mapper.writeValueAsString(sendMessage);
            sendMsg(value, client, post);
            // metrics
            outRecords.inc();
            if (outRecords.getCount() % ROW_PRINT_FREQUENCY == 0) {
                LOG.info(value);
            }
            TimeUnit.MILLISECONDS.sleep(httpTableInfo.getDelay());
        } catch (Exception e) {
            if (outDirtyRecords.getCount() % DIRTY_PRINT_FREQUENCY == 0 || LOG.isDebugEnabled()) {
                LOG.error("record insert failed ..{}", value);
                LOG.error("", e);
            }
            outDirtyRecords.inc();
        } finally {
            sendMessage.clear();
            if (client != null) {
                try {
                    client.close();
                } catch (IOException e) {
                    LOG.error(e.getMessage());
                }
            }
        }
    }

    /**
     * send data
     *
     * @param value
     * @param client
     * @param post
     * @throws Exception
     */
    private void sendMsg(String value, CloseableHttpClient client, HttpPost post) throws Exception {
        StringEntity stringEntity = new StringEntity(value);
        stringEntity.setContentEncoding("UTF-8");
        stringEntity.setContentType("application/json");
        post.setEntity(stringEntity);
        client.execute(post);
    }

    @Override
    public void close() {
    }

    public static class HttpOutputFormatBuilder {
        private final HttpOutputFormat httpOutputFormat;

        protected HttpOutputFormatBuilder() {
            this.httpOutputFormat = new HttpOutputFormat();
        }

        public HttpOutputFormatBuilder setHttpTableInfo(HttpTableInfo httpTableInfo) {
            httpOutputFormat.httpTableInfo = httpTableInfo;
            return this;
        }

        public HttpOutputFormat finish() {
            if (httpOutputFormat.httpTableInfo.getUrl() == null) {
                throw new IllegalArgumentException("No url supplied.");
            }
            return httpOutputFormat;
        }
    }
}
