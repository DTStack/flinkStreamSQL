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


package com.dtstack.flink.sql.launcher;

import com.dtstack.flink.sql.constrant.ConfigConstrant;
import com.google.common.collect.Lists;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.dtstack.flink.sql.enums.ClusterMode;
import com.dtstack.flink.sql.Main;
import com.dtstack.flink.sql.launcher.perjob.PerJobSubmitter;
import com.dtstack.flink.sql.option.OptionParser;
import com.dtstack.flink.sql.option.Options;
import com.dtstack.flink.sql.util.PluginUtil;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Date: 2017/2/20
 * Company: www.dtstack.com
 *
 * @author xuchao
 */

public class LauncherMain {
    private static final String CORE_JAR = "core";

    private static String SP = File.separator;

    private static final Logger LOG = LoggerFactory.getLogger(LauncherMain.class);


    private static String getLocalCoreJarPath(String localSqlRootJar) throws Exception {
        String jarPath = PluginUtil.getCoreJarFileName(localSqlRootJar, CORE_JAR);
        return localSqlRootJar + SP + jarPath;
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 1 && args[0].endsWith(".json")) {
            args = parseJson(args);
        }
        OptionParser optionParser = new OptionParser(args);
        Options launcherOptions = optionParser.getOptions();
        String mode = launcherOptions.getMode();
        List<String> argList = optionParser.getProgramExeArgList();

        String confProp = launcherOptions.getConfProp();
        confProp = URLDecoder.decode(confProp, Charsets.UTF_8.toString());
        Properties confProperties = PluginUtil.jsonStrToObject(confProp, Properties.class);

        LOG.info("current mode is {}", mode);

        if (mode.equals(ClusterMode.local.name())) {
            String[] localArgs = argList.toArray(new String[0]);
            Main.main(localArgs);
            return;
        }

        String pluginRoot = launcherOptions.getLocalSqlPluginPath();
        File jarFile = new File(getLocalCoreJarPath(pluginRoot));
        String[] remoteArgs = argList.toArray(new String[0]);
        PackagedProgram program = new PackagedProgram(jarFile, Lists.newArrayList(), remoteArgs);

        String savePointPath = confProperties.getProperty(ConfigConstrant.SAVE_POINT_PATH_KEY);
        if (StringUtils.isNotBlank(savePointPath)) {
            String allowNonRestoredState = confProperties.getOrDefault(ConfigConstrant.ALLOW_NON_RESTORED_STATE_KEY, "false").toString();
            program.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(savePointPath, BooleanUtils.toBoolean(allowNonRestoredState)));
        }

        if (mode.equals(ClusterMode.yarnPer.name())) {
            String flinkConfDir = launcherOptions.getFlinkconf();
            Configuration config = StringUtils.isEmpty(flinkConfDir) ? new Configuration() : GlobalConfiguration.loadConfiguration(flinkConfDir);
            JobGraph jobGraph = PackagedProgramUtils.createJobGraph(program, config, 1);

            LOG.info("current jobID is {}", jobGraph.getJobID());

            LOG.info("submit applicationId is {}", PerJobSubmitter.submit(launcherOptions, jobGraph, config));
        } else {
            ClusterClient clusterClient = ClusterClientFactory.createClusterClient(launcherOptions);
            clusterClient.run(program, 1);
            clusterClient.shutdown();
        }

    }

    private static String[] parseJson(String[] args) {
        BufferedReader reader = null;
        StringBuilder lastStr = new StringBuilder();
        try {
            FileInputStream fileInputStream = new FileInputStream(args[0]);
            InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream, Charsets.UTF_8);
            reader = new BufferedReader(inputStreamReader);
            String tempString;
            while ((tempString = reader.readLine()) != null) {
                lastStr.append(tempString);
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        Map<String, Object> map = JSON.parseObject(lastStr.toString(), new TypeReference<Map<String, Object>>() {
        });
        List<String> list = new LinkedList<>();

        for (Map.Entry<String, Object> entry : map.entrySet()) {
            list.add("-" + entry.getKey());
            list.add(entry.getValue().toString());
        }
        return list.toArray(new String[0]);
    }
}
