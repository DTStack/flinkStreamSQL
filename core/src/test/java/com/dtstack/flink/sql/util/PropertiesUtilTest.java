package com.dtstack.flink.sql.util;

import org.junit.Test;

import java.util.Properties;

public class PropertiesUtilTest {

    @Test
    public void propertiesTrim(){
        Properties properties = new Properties();
        properties.put("k", "v");
        PropertiesUtils.propertiesTrim(properties);
    }
}
