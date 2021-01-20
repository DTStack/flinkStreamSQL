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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.dtstack.flink.sql.Main;
import com.dtstack.flink.sql.dirtyManager.manager.DirtyDataManager;
import com.dtstack.flink.sql.enums.ClusterMode;
import com.dtstack.flink.sql.launcher.entity.JobParamsInfo;
import com.dtstack.flink.sql.launcher.executor.StandaloneExecutor;
import com.dtstack.flink.sql.launcher.executor.YarnJobClusterExecutor;
import com.dtstack.flink.sql.launcher.executor.YarnSessionClusterExecutor;
import com.dtstack.flink.sql.option.OptionParser;
import com.dtstack.flink.sql.option.Options;
import com.dtstack.flink.sql.util.PluginUtil;
import org.apache.commons.io.Charsets;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * Date: 2017/2/20
 * Company: www.dtstack.com
 * @author xuchao
 */

public class LauncherMain {


    @SuppressWarnings("unchecked")
    public static JobParamsInfo parseArgs(String[] args) throws Exception {
        if (args.length == 1 && args[0].endsWith(".json")) {
            args = parseJson(args);
        }
        OptionParser optionParser = new OptionParser(args);
        Options launcherOptions = optionParser.getOptions();
        List<String> programExeArgList = optionParser.getProgramExeArgList();
        String[] execArgs = programExeArgList.toArray(new String[0]);

        String name = launcherOptions.getName();
        String mode = launcherOptions.getMode();
        String localPluginRoot = launcherOptions.getLocalSqlPluginPath();
        String flinkConfDir = launcherOptions.getFlinkconf();
        String flinkJarPath = launcherOptions.getFlinkJarPath();
        String yarnConfDir = launcherOptions.getYarnconf();
        String udfJar = launcherOptions.getAddjar();
        String queue = launcherOptions.getQueue();
        String pluginLoadMode = launcherOptions.getPluginLoadMode();
        String addShipfile = launcherOptions.getAddShipfile();
        String dirtyStr = launcherOptions.getDirtyProperties();

        String yarnSessionConf = URLDecoder.decode(launcherOptions.getYarnSessionConf(), Charsets.UTF_8.toString());
        Properties yarnSessionConfProperties = PluginUtil.jsonStrToObject(yarnSessionConf, Properties.class);

        String confProp = URLDecoder.decode(launcherOptions.getConfProp(), Charsets.UTF_8.toString());
        Properties confProperties = PluginUtil.jsonStrToObject(confProp, Properties.class);
        Map<String, Object> dirtyProperties = PluginUtil.jsonStrToObject(Objects.isNull(dirtyStr) ?
               DirtyDataManager.buildDefaultDirty() : dirtyStr, Map.class);

        return JobParamsInfo.builder()
                .setExecArgs(execArgs)
                .setName(name)
                .setMode(mode)
                .setUdfJar(udfJar)
                .setLocalPluginRoot(localPluginRoot)
                .setFlinkConfDir(flinkConfDir)
                .setYarnConfDir(yarnConfDir)
                .setConfProperties(confProperties)
                .setYarnSessionConfProperties(yarnSessionConfProperties)
                .setFlinkJarPath(flinkJarPath)
                .setPluginLoadMode(pluginLoadMode)
                .setQueue(queue)
                .setDirtyProperties(dirtyProperties)
                .setAddShipfile(addShipfile)
                .build();
    }


    private static String[] parseJson(String[] args) {
        BufferedReader reader = null;
        StringBuilder lastStr = new StringBuilder();
        try {
            FileInputStream fileInputStream = new FileInputStream(args[0]);
            InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream, StandardCharsets.UTF_8);
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


    public static void main(String[] args) throws Exception {
        JobParamsInfo jobParamsInfo = parseArgs(args);
        ClusterMode execMode = ClusterMode.valueOf(jobParamsInfo.getMode());

        switch (execMode) {
            case local:
                Main.main(jobParamsInfo.getExecArgs());
                break;
            case yarn:
                new YarnSessionClusterExecutor(jobParamsInfo).exec();
                break;
            case yarnPer:
                new YarnJobClusterExecutor(jobParamsInfo).exec();
                break;
            case standalone:
                new StandaloneExecutor(jobParamsInfo).exec();
                break;
            default:
                throw new RuntimeException("Unsupported operating mode, please use local,yarn,yarnPer");
        }
    }
}
