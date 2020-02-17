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

package com.dtstack.flink.sql;

import com.dtstack.flink.sql.classloader.ClassLoaderManager;
import com.dtstack.flink.sql.enums.ClusterMode;
import com.dtstack.flink.sql.environment.MyLocalStreamEnvironment;
import com.dtstack.flink.sql.environment.StreamEnvConfigManager;
import com.dtstack.flink.sql.option.OptionParser;
import com.dtstack.flink.sql.option.Options;
import com.dtstack.flink.sql.parser.SqlParser;
import com.dtstack.flink.sql.parser.SqlTree;
import com.dtstack.flink.sql.side.SideTableInfo;
import com.dtstack.flink.sql.util.PluginUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.io.File;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 *  获取sql任务的执行计划
 * Date: 2020/2/17
 * Company: www.dtstack.com
 * @author maqi
 */
public class GetPlan {
    public static final String STATUS_KEY = "status";
    public static final String MSG_KEY = "msg";
    public static final Integer FAIL = 0;
    public static final Integer SUCCESS = 1;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static String getExecutionPlan(String[] args) {
        try {
            Arrays.stream(args).forEach(System.out::println);
            OptionParser optionParser = new OptionParser(args);
            Options options = optionParser.getOptions();
            String sql = options.getSql();
            String addJarListStr = options.getAddjar();
            String localSqlPluginPath = options.getLocalSqlPluginPath();
            String deployMode = ClusterMode.local.name();
            String confProp = options.getConfProp();

            sql = URLDecoder.decode(sql, Charsets.UTF_8.name());
            SqlParser.setLocalSqlPluginRoot(localSqlPluginPath);

            List<String> addJarFileList = Lists.newArrayList();
            if (!Strings.isNullOrEmpty(addJarListStr)) {
                addJarListStr = URLDecoder.decode(addJarListStr, Charsets.UTF_8.name());
                addJarFileList = OBJECT_MAPPER.readValue(addJarListStr, List.class);
            }

            confProp = URLDecoder.decode(confProp, Charsets.UTF_8.toString());
            Properties confProperties = PluginUtil.jsonStrToObject(confProp, Properties.class);
            StreamExecutionEnvironment env = CommonProcess.getStreamExeEnv(confProperties, deployMode);

            StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
            StreamQueryConfig streamQueryConfig = StreamEnvConfigManager.getStreamQueryConfig(tableEnv, confProperties);

            List<URL> jarUrlList = Lists.newArrayList();
            SqlTree sqlTree = SqlParser.parseSql(sql);

            //Get External jar to load
            for (String addJarPath : addJarFileList) {
                File tmpFile = new File(addJarPath);
                jarUrlList.add(tmpFile.toURI().toURL());
            }

            Map<String, SideTableInfo> sideTableMap = Maps.newHashMap();
            Map<String, Table> registerTableCache = Maps.newHashMap();

            //register udf
            CommonProcess.registerUserDefinedFunction(sqlTree, jarUrlList, tableEnv);
            //register table schema
            Set<URL> classPathSets = CommonProcess.registerTable(sqlTree, env, tableEnv, localSqlPluginPath, null, null, sideTableMap, registerTableCache);
            // cache classPathSets
            CommonProcess.registerPluginUrlToCachedFile(env, classPathSets);

            CommonProcess.sqlTranslation(localSqlPluginPath, tableEnv, sqlTree, sideTableMap, registerTableCache, streamQueryConfig);

            ((MyLocalStreamEnvironment)env).setClasspaths(ClassLoaderManager.getClassPath());

            String executionPlan = env.getExecutionPlan();
            return getJsonStr(SUCCESS, executionPlan);
        } catch (Exception e) {
            return getJsonStr(FAIL, ExceptionUtils.getFullStackTrace(e));
        }
    }

    public static String getJsonStr(int status, String msg) {
        ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
        objectNode.put(STATUS_KEY, status);
        objectNode.put(MSG_KEY, msg);
        return objectNode.toString();
    }
}
