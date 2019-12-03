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
package com.dtstack.flink.sql.sink;

import com.dtstack.flink.sql.config.DirtyConfig;
import com.dtstack.flink.sql.constrant.ConfigConstrant;
import com.dtstack.flink.sql.dirty.DirtyDataManager;
import com.dtstack.flink.sql.exception.ParseOrWriteRecordException;
import com.dtstack.flink.sql.metric.MetricConstant;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Created by sishu.yss on 2018/11/28.
 */
public abstract class MetricOutputFormat extends RichOutputFormat<Tuple2> {
    private static final Logger LOG = LoggerFactory.getLogger(MetricOutputFormat.class);

    private static final String SINK_DIRTYDATA_PREFIX = "sink";

    protected transient Counter outRecords;

    protected transient Counter outDirtyRecords;

    protected transient Meter outRecordsRate;

    protected  int dirtyDataPrintFrequency = 1000;
    protected  int receiveDataPrintFrequency = 1000;
    protected DirtyConfig dirtyConfig;
    protected DirtyDataManager dirtyDataManager;

    protected String jobId;

    public void initMetric() {
        outRecords = getRuntimeContext().getMetricGroup().counter(MetricConstant.DT_NUM_RECORDS_OUT);
        outDirtyRecords = getRuntimeContext().getMetricGroup().counter(MetricConstant.DT_NUM_DIRTY_RECORDS_OUT);
        outRecordsRate = getRuntimeContext().getMetricGroup().meter(MetricConstant.DT_NUM_RECORDS_OUT_RATE, new MeterView(outRecords, 20));
    }


    public void initDirtyDataOutputStream() {
        if (null != dirtyConfig) {
            Map<String, String> vars = getRuntimeContext().getMetricGroup().getAllVariables();
            if (vars != null && vars.get(ConfigConstrant.METRIC_JOB_ID) != null) {
                jobId = vars.get(ConfigConstrant.METRIC_JOB_ID);
            }
            dirtyDataManager = new DirtyDataManager(dirtyConfig);
            dirtyDataManager.createFsOutputStream(SINK_DIRTYDATA_PREFIX, jobId);
        }
    }

    /**
     *  对json解析失败时的异常处理
     * @param message
     * @param e
     * @throws IOException
     */
    protected void dealInsertError(String message, Exception e) {
        if (null != dirtyDataManager) {
            dirtyDataManager.writeData(new String(message), new ParseOrWriteRecordException(e.getMessage(), e));
        }

        if (null == dirtyDataManager && (outDirtyRecords.getCount() % dirtyDataPrintFrequency == 0)) {
            LOG.info("record insert failed .." + new String(message));
            LOG.error("", e);
        }
        outDirtyRecords.inc();
    }

    public void closeFsDataOutputStream() {
        if (null != dirtyDataManager) {
            dirtyDataManager.close();
        }
    }

    public void setDirtyConfig(DirtyConfig dirtyConfig) {
        this.dirtyConfig = dirtyConfig;
    }
}
