package com.dtstack.flink.sql.function;

import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.junit.Test;

import static org.mockito.Mockito.mock;

public class FunctionManagerTest extends ScalarFunction {

    @Test
    public void registerUDF(){
        StreamTableEnvironment tableEnvironment = mock(StreamTableEnvironment.class);

        FunctionManager.registerUDF("SCALA", "com.dtstack.flink.sql.function.FunctionManagerTest", "getResultType", tableEnvironment, Thread.currentThread().getContextClassLoader());
        FunctionManager.registerUDF("AGGREGATE", "org.apache.flink.table.planner.functions.aggfunctions.LastValueAggFunction", "resultTypeConvert", tableEnvironment, Thread.currentThread().getContextClassLoader());
        try{
            FunctionManager.registerUDF("TABLE", " org.apache.flink.table.plan.util.ObjectExplodeTableFunc", "collectArray", tableEnvironment, Thread.currentThread().getContextClassLoader());
        }catch (Exception e){

        }
    }

    @Test
    public void transformTypes(){
        Class[] fieldTypes = new Class[1];
        fieldTypes[0] = String.class;
        FunctionManager.transformTypes(fieldTypes);
    }

}
