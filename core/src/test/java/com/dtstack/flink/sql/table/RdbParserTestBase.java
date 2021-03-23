package com.dtstack.flink.sql.table;

import org.junit.Before;

/**
 * @program: flinkStreamSQL
 * @author: wuren
 * @create: 2020/11/09
 **/
abstract public class RdbParserTestBase {

    protected AbstractTableParser parser;
    protected String type;

    @Before
    abstract public void setUp();

}
