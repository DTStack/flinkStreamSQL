package com.dtstack.flink.sql.exec;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Mockito.mock;


@RunWith(PowerMockRunner.class)
@PrepareForTest({FlinkSQLExec.class,  Table.class})
public class FlinkSQLExecTest {


    @Test
    public void sqlUpdate() throws Exception {
        String stmt = "insert into a select fieldA from b";
        StreamTableEnvironment tableEnv = mock(StreamTableEnvironment.class);


    }



    @Test
    public void ignoreCase() throws NoSuchMethodException {
//        Method method = FlinkSQLExec.class.getDeclaredMethod("ignoreCase",String.class, String.class);
//        String[] queryFieldNames = new String[1];
//        queryFieldNames[0] = "a";
//        String[] sinkFieldNames = new String[1];
//        sinkFieldNames[0] = "a";
//        FlinkSQLExec.ignoreCase(queryFieldNames, sinkFieldNames);
    }

}
