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


package com.dtstack.flink.sql.watermarker;

import com.dtstack.flink.sql.metric.EventDelayGauge;
import com.dtstack.flink.sql.metric.MetricConstant;
import com.dtstack.flink.sql.util.MathUtil;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;

import java.util.TimeZone;

/**
 * Reason:
 * Date: 2018/10/18
 * Company: www.dtstack.com
 *
 * @author xuchao
 */

public abstract class AbstractCustomerWaterMarker<T> implements TimestampAssigner<T>, TimestampAssignerSupplier {


    private static final long serialVersionUID = 1L;

    private String fromSourceTag = "NONE";

    protected transient EventDelayGauge eventDelayGauge;

    protected int pos;

    protected long lastTime = 0;

    protected TimeZone timezone;

    @Override
    public TimestampAssigner createTimestampAssigner(Context context) {
        eventDelayGauge = new EventDelayGauge();
        context.getMetricGroup().getAllVariables().put("<source_tag>", fromSourceTag);
        context.getMetricGroup().gauge(MetricConstant.DT_EVENT_DELAY_GAUGE, eventDelayGauge);
        return this;
    }

    public void setFromSourceTag(String fromSourceTag) {
        this.fromSourceTag = fromSourceTag;
    }

    protected long getExtractTimestamp(Long extractTime) {

        lastTime = extractTime + timezone.getOffset(extractTime);

        eventDelayGauge.setDelayTime(MathUtil.getIntegerVal((System.currentTimeMillis() - extractTime) / 1000));

        return lastTime;
    }
}
