package com.dtstack.flink.sql.metric;

/**
 * Reason:
 * Date: 2018/10/18
 * Company: www.dtstack.com
 * @author xuchao
 */

public class MetricConstant {

    /**metric name of dirty data*/
    public static final String DT_DIRTY_DATA_COUNTER = "dtDirtyData";

    public static final String DT_NUM_RECORDS_IN_COUNTER = "dtNumRecordsIn";

    public static final String DT_NUM_RECORDS_IN_RATE = "dtNumRecordsInRate";

    public static final String DT_NUM_BYTES_IN_COUNTER = "dtNumBytesIn";

    public static final String DT_NUM_BYTES_IN_RATE = "dtNumBytesInRate";

    /**diff of DT_NUM_RECORD_IN_COUNTER ,this metric is desc record num after of deserialization*/
    public static final String DT_NUM_RECORDS_RESOVED_IN_COUNTER = "dtNumRecordsInResolve";

    public static final String DT_NUM_RECORDS_RESOVED_IN_RATE = "dtNumRecordsInResolveRate";

    public static final String DT_NUM_RECORDS_OUT = "dtNumRecordsOut";

    public static final String DT_NUM_RECORDS_OUT_RATE = "dtNumRecordsOutRate";
}
