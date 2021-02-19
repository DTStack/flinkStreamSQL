package com.dtstack.flink.sql.table;

import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.google.common.collect.Maps;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Map;

public class AbstractSideTableParserTest {

    @Test
    public void testarseCacheProp(){
        AbstractSideTableParser sideTableParserTest = new AbstractSideTableParser() {
            @Override
            public AbstractTableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) throws Exception {
                return null;
            }
        };
        AbstractSideTableInfo sideTableInfo = Mockito.mock(AbstractSideTableInfo.class);
        Map<String, Object> props = Maps.newHashMap();
        props.put(AbstractSideTableInfo.CACHE_KEY.toLowerCase(), "all");
        props.put(AbstractSideTableInfo.CACHE_SIZE_KEY.toLowerCase(), 1000);
        props.put(AbstractSideTableInfo.CACHE_TTLMS_KEY.toLowerCase(), 10000);
        props.put(AbstractSideTableInfo.PARTITIONED_JOIN_KEY.toLowerCase(), false);
        props.put(AbstractSideTableInfo.CACHE_MODE_KEY.toLowerCase(), "ordered");
        props.put(AbstractSideTableInfo.ASYNC_CAP_KEY.toLowerCase(), 100);
        props.put(AbstractSideTableInfo.ASYNC_TIMEOUT_KEY.toLowerCase(), 1000);
        props.put(AbstractSideTableInfo.ASYNC_FAIL_MAX_NUM_KEY.toLowerCase(), 12);
        props.put(AbstractSideTableInfo.CONNECT_RETRY_MAX_NUM_KEY.toLowerCase(),3);
        sideTableParserTest.parseCacheProp(sideTableInfo, props);
    }


}
