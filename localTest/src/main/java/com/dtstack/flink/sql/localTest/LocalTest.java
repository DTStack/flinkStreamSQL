/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flink.sql.localTest;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
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
import java.util.TimeZone;

/**
 * @author tiezhu
 * @Date 2020/7/8 Wed
 * Company dtstack
 */
public class LocalTest {

    private static final Logger LOG = LoggerFactory.getLogger(LocalTest.class);

    public static void main(String[] args) throws Exception {

        setLogLevel("INFO");

        List<String> propertiesList = new ArrayList<>();
        String sqlPath = "/Users/wtz/dtstack/sql/test/JoinDemoOne.sql";
        Map<String, Object> conf = new HashMap<>();
        JSONObject properties = new JSONObject();

        //其他参数配置
        properties.put("time.characteristic", "eventTime");
        properties.put("timezone", TimeZone.getDefault());
        properties.put("early.trigger", "1");

        // 任务配置参数
        conf.put("-sql", URLEncoder.encode(readSQL(sqlPath), StandardCharsets.UTF_8.name()));
        conf.put("-mode", "local");
        conf.put("-name", "flinkStreamSQLLocalTest");
        conf.put("-confProp", properties.toString());
        conf.put("-pluginLoadMode", "LocalTest");
        conf.put("-planner", "flink");
        conf.put("-dirtyProperties", buildDirtyStr());

        for (Map.Entry<String, Object> keyValue : conf.entrySet()) {
            propertiesList.add(keyValue.getKey());
            propertiesList.add(keyValue.getValue().toString());
        }

        Main.main(propertiesList.toArray(new String[0]));
    }

    private static String buildDirtyStr() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("type", "console");
        // 多少条数据打印一次
        jsonObject.put("printLimit", "100");
        jsonObject.put("url", "jdbc:mysql://localhost:3306/tiezhu");
        jsonObject.put("userName", "root");
        jsonObject.put("password", "abc123");
        jsonObject.put("isCreateTable", "false");
        // 多少条数据写入一次
        jsonObject.put("batchSize", "1");
        jsonObject.put("tableName", "dirtyData");

        return jsonObject.toJSONString();

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

    private static void setLogLevel(String level) {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        ch.qos.logback.classic.Logger logger = loggerContext.getLogger("root");
        logger.setLevel(Level.toLevel(level));
    }
}
