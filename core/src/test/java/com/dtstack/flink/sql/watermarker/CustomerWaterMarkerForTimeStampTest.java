package com.dtstack.flink.sql.watermarker;

import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.types.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Time.class})
public class CustomerWaterMarkerForTimeStampTest {

        @Test
        public void extractTimestamp(){
            Time time = mock(Time.class);
            when(time.toMilliseconds()).thenReturn(121200000l);
            CustomerWaterMarkerForTimeStamp customerWaterMarkerForTimeStamp = new CustomerWaterMarkerForTimeStamp(time, 1,"");

            Row row = mock(Row.class);
            when(row.getField(1)).thenReturn(System.currentTimeMillis());
            customerWaterMarkerForTimeStamp.extractTimestamp(row);
        }
}
