package com.dtstack.flink.sql.outputformat;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AbstractDtRichOutputFormatTest {

    @Test
    public void initMetric(){
        AbstractDtRichOutputFormat richOutputFormat = new AbstractDtRichOutputFormat() {
            @Override
            public void configure(Configuration parameters) {

            }

            @Override
            public void open(int taskNumber, int numTasks) throws IOException {

            }

            @Override
            public void writeRecord(Object record) throws IOException {

            }

            @Override
            public void close() throws IOException {

            }
        };
        RuntimeContext runtimeContext = mock(RuntimeContext.class);
        richOutputFormat.setRuntimeContext(runtimeContext);
        MetricGroup metricGroup = mock(MetricGroup.class);
        when(runtimeContext.getMetricGroup()).thenReturn(metricGroup);
        richOutputFormat.initMetric();
    }
}
