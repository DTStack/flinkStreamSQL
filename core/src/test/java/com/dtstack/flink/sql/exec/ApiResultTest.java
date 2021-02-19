package com.dtstack.flink.sql.exec;

import org.junit.Test;

public class ApiResultTest {

    @Test
    public void createSuccessResultJsonStr(){
        ApiResult.createSuccessResultJsonStr("ss", 12L);
    }
    @Test
    public void createErrorResultJsonStr(){
        ApiResult.createErrorResultJsonStr("ss");
    }


}
