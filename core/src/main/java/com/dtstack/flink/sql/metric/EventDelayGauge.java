package com.dtstack.flink.sql.metric;

import org.apache.flink.metrics.Gauge;

/**
 * 数据延迟时间 单位 s
 * Date: 2018/10/18
 * Company: www.dtstack.com
 * @author xuchao
 */

public class EventDelayGauge implements Gauge<Integer> {

    private volatile int delayTime = 0;

    public void setDelayTime(int delayTime) {
        this.delayTime = delayTime;
    }

    @Override
    public Integer getValue() {
        return delayTime;
    }
}
