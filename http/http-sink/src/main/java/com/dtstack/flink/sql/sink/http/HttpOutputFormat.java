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
import com.dtstack.flink.sql.util.PluginUtil;
import com.dtstack.flink.sql.util.ThreadUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;

/**
 * @author: chuixue
 * @create: 2021-03-03 10:41
 * @description:
 **/
public class HttpOutputFormat extends AbstractDtRichOutputFormat<Tuple2<Boolean, Row>> {
    private static final Logger LOG = LoggerFactory.getLogger(HttpOutputFormat.class);

    /**
     * Default connection timeout when connecting to the server socket (infinite).
     */
    private HttpTableInfo httpTableInfo;

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
    }

    @Override
    public void writeRecord(Tuple2<Boolean, Row> record) {
        if (!record.f0) {
            return;
        }

        String value = null;
        HashMap<String, Object> map = new HashMap<>();
        try {
            // not empty ,need send flag、tablename、fieldinfo、value.
            if (StringUtils.isNotEmpty(httpTableInfo.getFlag())) {
                map.put("flag", httpTableInfo.getFlag());
                map.put("tableName", httpTableInfo.getName());
            }

            // add field
            String[] fields = httpTableInfo.getFields();
            Row row = record.getField(1);
            for (int i = 0; i < fields.length; i++) {
                map.put(fields[i], row.getField(i));
            }

            // post request
            value = PluginUtil.objectToString(map);
            DtHttpClient.post(httpTableInfo.getUrl(), value);

            // metrics
            outRecords.inc();
            if (outRecords.getCount() % ROW_PRINT_FREQUENCY == 0) {
                LOG.info("row data: {}", value);
            }
            ThreadUtil.sleepMilliseconds(httpTableInfo.getDelay());
        } catch (IOException e) {
            if (outDirtyRecords.getCount() % DIRTY_PRINT_FREQUENCY == 0 || LOG.isDebugEnabled()) {
                LOG.error("record insert failed ..{}", value);
                LOG.error("", e);
            }
            outDirtyRecords.inc();
        }
    }

    @Override
    public void close() {
        DtHttpClient.close();
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
            Preconditions.checkNotNull(httpOutputFormat.httpTableInfo.getUrl(), "No url supplied.");
            return httpOutputFormat;
        }
    }
}
