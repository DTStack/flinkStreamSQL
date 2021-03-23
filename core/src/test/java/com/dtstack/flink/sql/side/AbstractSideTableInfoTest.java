package com.dtstack.flink.sql.side;

import org.junit.Test;

public class AbstractSideTableInfoTest {

    @Test
    public void getRowTypeInfo(){
        AbstractSideTableInfo sideTableInfo = new AbstractSideTableInfo() {
            @Override
            public boolean check() {
                return false;
            }
        };
        Class[] fieldClasses = new Class[1];
        fieldClasses[0] = String.class;
        sideTableInfo.setFieldClasses(fieldClasses);

        String[] fields = new String[1];
        fields[0] = "a";

        sideTableInfo.setFields(fields);
        sideTableInfo.getRowTypeInfo();
    }
}
