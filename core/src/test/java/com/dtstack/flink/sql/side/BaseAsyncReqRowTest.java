package com.dtstack.flink.sql.side;

import com.dtstack.flink.sql.enums.ECacheContentType;
import com.dtstack.flink.sql.metric.MetricConstant;
import com.dtstack.flink.sql.side.cache.AbstractSideCache;
import com.dtstack.flink.sql.side.cache.CacheObj;
import com.google.common.collect.Lists;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Map;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BaseAsyncReqRowTest {

    private BaseAsyncReqRow asyncReqRow;

    @Before
    public void init() throws Exception {
        BaseSideInfo sideInfo = mock(BaseSideInfo.class);
        AbstractSideCache sideCache = mock(AbstractSideCache.class);
        when(sideInfo.getSideCache()).thenReturn(sideCache);
        when(sideCache.getFromCache(anyString())).thenReturn(CacheObj.buildCacheObj(ECacheContentType.SingleLine, "1"));
        AbstractSideTableInfo sideTableInfo = mock(AbstractSideTableInfo.class);
        when(sideInfo.getSideTableInfo()).thenReturn(sideTableInfo);
        when(sideTableInfo.getCacheType()).thenReturn("lru");

        when(sideInfo.getEqualValIndex()).thenReturn(Lists.newArrayList(0));
        when(sideInfo.getEqualFieldList()).thenReturn(Lists.newArrayList("key"));

        asyncReqRow = new BaseAsyncReqRow(sideInfo) {
            @Override
            public BaseRow fillData(BaseRow input, Object sideInput) {
                return null;
            }

            @Override
            public void handleAsyncInvoke(Map<String, Object> inputParams, BaseRow input, ResultFuture<BaseRow> resultFuture) throws Exception {

            }

            @Override
            public String buildCacheKey(Map<String, Object> inputParams) {
                return "key";
            }
        };

        StreamingRuntimeContext runtimeContext = mock(StreamingRuntimeContext.class);
        MetricGroup metricGroup = mock(MetricGroup.class);
        when(runtimeContext.getMetricGroup()).thenReturn(metricGroup);
        Counter counter = mock(Counter.class);
        when(metricGroup.counter(MetricConstant.DT_NUM_SIDE_PARSE_ERROR_RECORDS)).thenReturn(counter);

        ProcessingTimeService processingTimeService = mock(ProcessingTimeService.class);
        when(runtimeContext.getProcessingTimeService()).thenReturn(processingTimeService);
        when(processingTimeService.getCurrentProcessingTime()).thenReturn(System.currentTimeMillis());


        asyncReqRow.setRuntimeContext(runtimeContext);

    }

    // @Test
    public void testAllMethod() throws Exception {
        Configuration configuration = Mockito.mock(Configuration.class);
        asyncReqRow.open(configuration);
        GenericRow input = new GenericRow(1);
        input.setField(0, "a");
        ResultFuture<BaseRow> resultFuture = mock(ResultFuture.class);
        asyncReqRow.dealMissKey(input, resultFuture);

        asyncReqRow.dealCacheData("key", CacheObj.buildCacheObj(ECacheContentType.SingleLine, "a") );
        asyncReqRow.getFromCache("key");

        asyncReqRow.timeout(input, resultFuture);


        asyncReqRow.asyncInvoke(input, resultFuture);
        asyncReqRow.dealFillDataError(input, resultFuture, new RuntimeException(""));

    }

}
