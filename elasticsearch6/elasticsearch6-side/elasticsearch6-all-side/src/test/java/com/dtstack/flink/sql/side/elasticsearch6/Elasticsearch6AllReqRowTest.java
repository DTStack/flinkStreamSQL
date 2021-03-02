package com.dtstack.flink.sql.side.elasticsearch6;


import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.junit.Test;

import static org.mockito.Mockito.mock;

/**
 * Company: www.dtstack.com
 *
 * @author zhufeng
 * @date 2020-07-03
 */
public class Elasticsearch6AllReqRowTest {
    Elasticsearch6AllReqRow reqRow = mock(Elasticsearch6AllReqRow.class);

    @Test
//有问题，有参构造器初始化时getKind()中Sqlkind.inter无法赋值
    public void ES6Test() throws Exception {
        BaseRow value = mock(BaseRow.class);
        Collector<BaseRow> out = mock(Collector.class);
        BaseRow input = mock(BaseRow.class);
        Object sideInput = mock(Object.class);
        reqRow.initCache();
        reqRow.getFetchSize();
        reqRow.reloadCache();
        reqRow.flatMap(value, out);
        reqRow.fillData(input, sideInput);
    }

}
