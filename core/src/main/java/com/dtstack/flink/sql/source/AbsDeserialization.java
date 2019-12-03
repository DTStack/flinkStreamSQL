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

package com.dtstack.flink.sql.source;

import com.dtstack.flink.sql.config.DirtyConfig;
import com.dtstack.flink.sql.constrant.ConfigConstrant;
import com.dtstack.flink.sql.dirty.DirtyDataManager;
import com.dtstack.flink.sql.exception.ParseOrWriteRecordException;
import com.dtstack.flink.sql.metric.MetricConstant;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * add metric for source, customer Deserialization which want add metric need to extends this abs class
 * Date: 2018/10/19
 * Company: www.dtstack.com
 *
 * @author xuchao
 */

public abstract class AbsDeserialization<T> extends AbstractDeserializationSchema<T> {

    private static final Logger LOG = LoggerFactory.getLogger(AbsDeserialization.class);
    private static final long serialVersionUID = 2176278128811784415L;

    private static final String SOURCE_DIRTYDATA_PREFIX = "source";

    private transient RuntimeContext runtimeContext;

    protected transient Counter dirtyDataCounter;

    //tps ransactions Per Second
    protected transient Counter numInRecord;

    protected transient Meter numInRate;

    //rps Record Per Second: deserialize data and out record num
    protected transient Counter numInResolveRecord;

    protected transient Meter numInResolveRate;

    protected transient Counter numInBytes;

    protected transient Meter numInBytesRate;

    protected DirtyConfig dirtyConfig;
    protected DirtyDataManager dirtyDataManager;

    protected int dirtyDataFrequency = 1000;

    protected String jobId;

    public RuntimeContext getRuntimeContext() {
        return runtimeContext;
    }

    public void setRuntimeContext(RuntimeContext runtimeContext) {
        this.runtimeContext = runtimeContext;
    }

    public void initMetric() {
        dirtyDataCounter = runtimeContext.getMetricGroup().counter(MetricConstant.DT_DIRTY_DATA_COUNTER);

        numInRecord = runtimeContext.getMetricGroup().counter(MetricConstant.DT_NUM_RECORDS_IN_COUNTER);
        numInRate = runtimeContext.getMetricGroup().meter(MetricConstant.DT_NUM_RECORDS_IN_RATE, new MeterView(numInRecord, 20));

        numInBytes = runtimeContext.getMetricGroup().counter(MetricConstant.DT_NUM_BYTES_IN_COUNTER);
        numInBytesRate = runtimeContext.getMetricGroup().meter(MetricConstant.DT_NUM_BYTES_IN_RATE, new MeterView(numInBytes, 20));

        numInResolveRecord = runtimeContext.getMetricGroup().counter(MetricConstant.DT_NUM_RECORDS_RESOVED_IN_COUNTER);
        numInResolveRate = runtimeContext.getMetricGroup().meter(MetricConstant.DT_NUM_RECORDS_RESOVED_IN_RATE, new MeterView(numInResolveRecord, 20));
    }

    public void initDirtyDataOutputStream() {
        if (null != dirtyConfig) {
            Map<String, String> vars = runtimeContext.getMetricGroup().getAllVariables();
            if (vars != null && vars.get(ConfigConstrant.METRIC_JOB_ID) != null) {
                jobId = vars.get(ConfigConstrant.METRIC_JOB_ID);
            }
            dirtyDataManager = new DirtyDataManager(dirtyConfig);
            dirtyDataManager.createFsOutputStream(SOURCE_DIRTYDATA_PREFIX, jobId);
        }
    }

    /**
     *  对json解析失败时的异常处理
     * @param message
     * @param e
     * @throws IOException
     */
    protected void dealParseError(byte[] message, Exception e) throws IOException {
        if (null != dirtyDataManager) {
            dirtyDataManager.writeData(new String(message), new ParseOrWriteRecordException(e.getMessage(), e));
        }

        if (null == dirtyDataManager && (dirtyDataCounter.getCount() % dirtyDataFrequency == 0)) {
            LOG.info("dirtyData: " + new String(message));
            LOG.error("", e);
        }
        dirtyDataCounter.inc();
    }

    public void closefsDataOutputStream() {
        if (null != dirtyDataManager) {
            dirtyDataManager.close();
        }
    }

    public void setDirtyConfig(DirtyConfig dirtyConfig) {
        this.dirtyConfig = dirtyConfig;
    }
}
