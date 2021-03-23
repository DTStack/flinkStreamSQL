package com.dtstack.flink.sql.sink.elasticsearch;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Company: www.dtstack.com
 *
 * @author zhufeng
 * @date 2020-06-18
 */
public class CustomerSinkFuncTest {
    private CustomerSinkFunc customerSinkFunc;
    @Mock
    RuntimeContext ctx;
    @Mock
    RequestIndexer indexer;
    @Mock
    Counter outRecords;
    String index;
    String type;
    List<String> fieldNames = Collections.singletonList("name.pv");
    List<String> fieldTypes = Arrays.asList("varchar", "varchar");
    List<Object> idFieldIndexes = Collections.singletonList(1);
    Tuple2<Boolean, Row> tuple2 = new Tuple2<>();

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        customerSinkFunc = new CustomerSinkFunc(index, type, fieldNames, fieldTypes, idFieldIndexes, true);
        customerSinkFunc.setOutRecords(outRecords);
        tuple2.setField(true, 0);
        Row row = new Row(1);
        row.setField(0, 1);
        tuple2.setField(row, 1);
    }

    //测试CustomerSinkFunc中的process和createIndexRequest方法
    @Test
    public void processAndcreateIndexRequestTest() {
        customerSinkFunc.process(tuple2, ctx, indexer);
        tuple2.setField(false, 0);
        customerSinkFunc.process(tuple2, ctx, indexer);
    }

}
