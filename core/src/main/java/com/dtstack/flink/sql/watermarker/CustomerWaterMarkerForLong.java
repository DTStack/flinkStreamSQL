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

import com.dtstack.flink.sql.util.MathUtil;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TimeZone;

/**
 * Custom watermark --- for eventtime
 * Date: 2017/12/28
 * Company: www.dtstack.com
 * @author xuchao
 */

public class CustomerWaterMarkerForLong extends AbsCustomerWaterMarker<Row> {

    private static final Logger logger = LoggerFactory.getLogger(CustomerWaterMarkerForLong.class);

    private static final long serialVersionUID = 1L;

    private int pos;

    private long lastTime = 0;

    private TimeZone timezone;

    public CustomerWaterMarkerForLong(Time maxOutOfOrderness, int pos,String timezone) {
        super(maxOutOfOrderness);
        this.pos = pos;
        this.timezone= TimeZone.getTimeZone(timezone);
    }

    @Override
    public long extractTimestamp(Row row) {

        try{
            Long extractTime = MathUtil.getLongVal(row.getField(pos));

            lastTime = extractTime + timezone.getOffset(extractTime);

            eventDelayGauge.setDelayTime(MathUtil.getIntegerVal((System.currentTimeMillis() - extractTime)/1000));

            return lastTime;
        }catch (Exception e){
            logger.error("", e);
        }
        return lastTime;
    }

}
