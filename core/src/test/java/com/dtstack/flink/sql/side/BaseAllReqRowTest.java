package com.dtstack.flink.sql.side;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.SQLException;

public class BaseAllReqRowTest {

    @Test
    public void testBaseAllReqRow() throws Exception {
        BaseSideInfo sideInfo = Mockito.mock(BaseSideInfo.class);
        AbstractSideTableInfo sideTableInfo = Mockito.mock(AbstractSideTableInfo.class);

        Mockito.when(sideInfo.getSideTableInfo()).thenReturn(sideTableInfo);
        Mockito.when(sideTableInfo.getCacheTimeout()).thenReturn(10L);
        BaseAllReqRow baseAllReqRow = new BaseAllReqRow(sideInfo) {
            @Override
            public void flatMap(BaseRow value, Collector<BaseRow> out) throws Exception {

            }

            @Override
            protected void initCache() throws SQLException {

            }

            @Override
            protected void reloadCache() {

            }
        };

        Configuration configuration = Mockito.mock(Configuration.class);
        baseAllReqRow.open(configuration);
        BaseRow value = Mockito.mock(BaseRow.class);
        Collector<BaseRow> out = Mockito.mock(Collector.class);
        baseAllReqRow.sendOutputRow(value, null, out);
        baseAllReqRow.close();
    }

}
