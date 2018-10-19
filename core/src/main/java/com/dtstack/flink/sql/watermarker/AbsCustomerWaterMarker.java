package com.dtstack.flink.sql.watermarker;

import com.dtstack.flink.sql.metric.EventDelayGauge;
import com.dtstack.flink.sql.metric.MetricConstant;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Reason:
 * Date: 2018/10/18
 * Company: www.dtstack.com
 * @author xuchao
 */

public abstract class AbsCustomerWaterMarker<T> extends BoundedOutOfOrdernessTimestampExtractor<T> implements RichFunction {


    private static final long serialVersionUID = 1L;

    private String fromSourceTag = "NONE";

    private transient RuntimeContext runtimeContext;

    protected transient EventDelayGauge eventDelayGauge;

    public AbsCustomerWaterMarker(Time maxOutOfOrderness) {
        super(maxOutOfOrderness);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
    }

    @Override
    public void close() throws Exception {
        //do nothing
    }

    @Override
    public RuntimeContext getRuntimeContext() {
        if (this.runtimeContext != null) {
            return this.runtimeContext;
        } else {
            throw new IllegalStateException("The runtime context has not been initialized.");
        }
    }

    @Override
    public IterationRuntimeContext getIterationRuntimeContext() {
        if (this.runtimeContext == null) {
            throw new IllegalStateException("The runtime context has not been initialized.");
        } else if (this.runtimeContext instanceof IterationRuntimeContext) {
            return (IterationRuntimeContext) this.runtimeContext;
        } else {
            throw new IllegalStateException("This stub is not part of an iteration step function.");
        }
    }

    @Override
    public void setRuntimeContext(RuntimeContext t) {
        this.runtimeContext = t;
        eventDelayGauge = new EventDelayGauge();
        t.getMetricGroup().getAllVariables().put("<source_tag>", fromSourceTag);
        t.getMetricGroup().gauge(MetricConstant.DT_EVENT_DELAY_GAUGE, eventDelayGauge);
    }

    public void setFromSourceTag(String fromSourceTag) {
        this.fromSourceTag = fromSourceTag;
    }
}
