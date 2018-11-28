package com.dtstack.flink.sql.metric;

import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;

/**
 * Created by sishu.yss on 2018/11/28.
 */
public abstract  class MetricOutputFormat extends  RichOutputFormat<Tuple2>{

     protected  transient Counter outRecords;

     protected transient Meter outRecordsRate;

     public void initMetric() {
        outRecords = getRuntimeContext().getMetricGroup().counter(MetricConstant.DT_NUM_RECORDS_OUT);
        outRecordsRate = getRuntimeContext().getMetricGroup().meter(MetricConstant.DT_NUM_RECORDS_OUT_RATE, new MeterView(outRecords, 20));
     }

}
