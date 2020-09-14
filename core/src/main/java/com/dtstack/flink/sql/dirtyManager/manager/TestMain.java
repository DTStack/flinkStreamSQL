package com.dtstack.flink.sql.dirtyManager.manager;

import java.util.HashMap;
import java.util.Map;

/**
 * @author tiezhu
 * Company dtstack
 * Date 2020/8/28 星期五
 */
public class TestMain {
    private static final Integer DATA_NUMBER = 1000;

    public static void main(String[] args) throws Exception {
        Map<String, String> properties = new HashMap<>(8);
        properties.put("type", "mysql");
        properties.put("pluginPath", "/Users/wtz/IdeaProjects/flinkStreamSQLTemp/sqlplugins");
        properties.put("url", "jdbc:mysql://kerberos01:3306/tiezhu");
        properties.put("userName", "dtstack");
        properties.put("password", "abc123");
        properties.put("isCreatedTable", "false");
        properties.put("batchSize", "1");
        properties.put("tableName", "DirtyDataFromMysql_2020_09_14_10_51_50");

        DirtyDataManager manager = DirtyDataManager.newInstance(properties);
        for (int i = 0; i < DATA_NUMBER; i++) {
            Thread.sleep(100);
            manager.collectDirtyData("testDirtyData" + i,
                    new Exception("testException" + i).getMessage(), "testField");
            if (i == 500) {
                manager.close();
            }
        }
    }
}
