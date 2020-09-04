package com.dtstack.flink.sql.dirtyManager.manager;

import java.util.HashMap;
import java.util.Map;

/**
 * @author tiezhu
 * Company dtstack
 * Date 2020/8/28 星期五
 */
public class TestMain {
    public static void main(String[] args) throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "console");
        properties.put("pluginPath", "/Users/wtz/IdeaProjects/flinkStreamSQL/sqlplugins");

        DirtyDataManager manager = DirtyDataManager.newInstance(properties);
        for (int i = 0; i < 100; i++) {
            Thread.sleep(1000);
            manager.collectDirtyData("testDirtyData", new Exception("testException").getMessage(), "testField");
        }
    }
}
