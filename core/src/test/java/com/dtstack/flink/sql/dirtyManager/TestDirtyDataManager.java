package com.dtstack.flink.sql.dirtyManager;

import com.dtstack.flink.sql.dirtyManager.consumer.AbstractDirtyDataConsumer;
import com.dtstack.flink.sql.dirtyManager.manager.DirtyDataManager;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author tiezhu
 * Company dtstack
 * Date 2020/8/28 星期五
 */
public class TestDirtyDataManager {
    @Test
    public void testCreateConsumer() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "print");
        properties.put("pluginPath", "/Users/wtz/IdeaProjects/flinkStreamSQL/sqlplugins");

        DirtyDataManager manager = DirtyDataManager.newInstance();
       AbstractDirtyDataConsumer consumer = manager.createConsumer(properties);
        Assert.assertNotNull(consumer);
    }
}
