package com.dtstack.flink.sql.localTest;

import com.alibaba.fastjson.JSONObject;
import com.dtstack.flink.sql.Main;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author tiezhu
 * @Date 2020/7/8 Wed
 * Company dtstack
 */
public class LocalTest {

    private static final Logger LOG = LoggerFactory.getLogger(LocalTest.class);

    public static void main(String[] args) throws Exception {

        List<String> propertiesList = new ArrayList<>();
        String sqlPath = "/Users/wtz4680/Desktop/flinkStreamSQL/Bug/Redmine_25807.sql";
        Map<String, Object> conf = new HashMap<>();
        JSONObject properties = new JSONObject();

        //其他参数配置
        properties.put("time.characteristic", "eventTime");
        
        // 任务配置参数
        conf.put("-sql", URLEncoder.encode(readSQL(sqlPath), StandardCharsets.UTF_8.name()));
        conf.put("-mode", "local");
        conf.put("-name", "flinkStreamSQLLocalTest");
        conf.put("-confProp", properties.toString());
        conf.put("-pluginLoadMode", "CLASSPATH");

        for (Map.Entry<String, Object> keyValue : conf.entrySet()) {
            propertiesList.add(keyValue.getKey());
            propertiesList.add(keyValue.getValue().toString());
        }

        Main.main(propertiesList.toArray(new String[0]));
    }

    private static String readSQL(String sqlPath) {
        try {
            byte[] array = Files.readAllBytes(Paths.get(sqlPath));
            return new String(array, StandardCharsets.UTF_8);
        } catch (IOException ioe) {
            LOG.error("Can not get the job info !!!", ioe);
            throw new RuntimeException(ioe);
        }
    }
}
