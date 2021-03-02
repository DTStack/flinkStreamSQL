package com.dtstack.flink.sql.sink.elasticsearch;

import junit.framework.Assert;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;


/**
 * Company: www.dtstack.com
 *
 * @author zhufeng
 * @date 2020-06-23
 */
public class Es6UtilTest {
    List<String> fieldNames = Collections.singletonList("name.pv");
    List<String> fieldTypes = Arrays.asList("varchar", "varchar");
    Row row = new Row(1);

    @Test
    public void rowToJsonMapTest() {
        boolean test = false;
        if (Es6Util.rowToJsonMap(row, fieldNames, fieldTypes).toString().equals("{name={}}")) {
            test = true;
        }
        Assert.assertTrue(test);
    }
}
