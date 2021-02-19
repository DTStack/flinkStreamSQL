package com.dtstack.flink.sql.sink.elasticsearch;

import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;


/**
 * Company: www.dtstack.com
 *
 * @author zhufeng
 * @date 2020-06-19
 */

public class EsUtilTest {
    List<String> fieldNames = Collections.singletonList("name.pv");
    List<String> fieldTypes = Arrays.asList("varchar", "varchar");
    Row row = new Row(1);

    @Test
    public void rowToJsonMapTest() {
        boolean test = false;
        if (EsUtil.rowToJsonMap(row, fieldNames, fieldTypes).toString().equals("{name={}}")) {
            test = true;
        }
        Assert.assertTrue(test);

    }

}
