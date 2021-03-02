package com.dtstack.flink.sql.factory;

import org.junit.Test;

public class DTFactoryTest {

    @Test
    public void testFactory(){
        DTThreadFactory dtThreadFactory = new DTThreadFactory("test");
        dtThreadFactory.newThread(new Runnable() {
            @Override
            public void run() {
                System.out.println("run..");
            }
        });
    }
}
