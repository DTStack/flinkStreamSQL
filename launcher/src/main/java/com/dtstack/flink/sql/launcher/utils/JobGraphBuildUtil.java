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

package com.dtstack.flink.sql.launcher.utils;

import com.dtstack.flink.sql.constrant.ConfigConstrant;
import com.dtstack.flink.sql.launcher.entity.JobParamsInfo;
import com.dtstack.flink.sql.util.MathUtil;
import com.dtstack.flink.sql.util.PluginUtil;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang.BooleanUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.util.function.FunctionUtils;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 *  build JobGraph by JobParamsInfo
 * Date: 2020/3/4
 * Company: www.dtstack.com
 * @author maqi
 */
public class JobGraphBuildUtil {

    private static final String SP = File.separator;
    private static final String CORE_JAR = "core";

    private static String findLocalCoreJarPath(String localSqlRootJar, String pluginLoadMode) throws Exception {
        String jarPath = PluginUtil.getCoreJarFileName(localSqlRootJar, CORE_JAR, pluginLoadMode);
        return localSqlRootJar + SP + jarPath;
    }

    public static JobGraph buildJobGraph(JobParamsInfo jobParamsInfo) throws Exception {
        Properties confProperties = jobParamsInfo.getConfProperties();
        int parallelism = MathUtil.getIntegerVal(confProperties.getProperty(ConfigConstrant.SQL_ENV_PARALLELISM, "1"));
        String flinkConfDir = jobParamsInfo.getFlinkConfDir();

        String[] execArgs = jobParamsInfo.getExecArgs();
        File coreJarFile = new File(findLocalCoreJarPath(jobParamsInfo.getLocalPluginRoot(), jobParamsInfo.getPluginLoadMode()));
        SavepointRestoreSettings savepointRestoreSettings = dealSavepointRestoreSettings(jobParamsInfo.getConfProperties());

        PackagedProgram program = PackagedProgram.newBuilder()
                .setJarFile(coreJarFile)
                .setArguments(execArgs)
                .setSavepointRestoreSettings(savepointRestoreSettings)
                .build();

        Configuration flinkConfig = getFlinkConfiguration(flinkConfDir, confProperties);
        JobGraph jobGraph = PackagedProgramUtils.createJobGraph(program, flinkConfig, parallelism, false);
        return jobGraph;
    }


    protected static SavepointRestoreSettings dealSavepointRestoreSettings(Properties confProperties) {
        SavepointRestoreSettings savepointRestoreSettings = SavepointRestoreSettings.none();
        String savePointPath = confProperties.getProperty(ConfigConstrant.SAVE_POINT_PATH_KEY);
        if (StringUtils.isNotBlank(savePointPath)) {
            String allowNonRestoredState = confProperties.getOrDefault(ConfigConstrant.ALLOW_NON_RESTORED_STATE_KEY, "false").toString();
            savepointRestoreSettings = SavepointRestoreSettings.forPath(savePointPath, BooleanUtils.toBoolean(allowNonRestoredState));
        }
        return savepointRestoreSettings;
    }

    public static Configuration getFlinkConfiguration(String flinkConfDir, Properties confProperties) {
        Configuration flinkConfig = StringUtils.isEmpty(flinkConfDir) ? new Configuration() : GlobalConfiguration.loadConfiguration(flinkConfDir);

        confProperties.forEach((key, val) -> flinkConfig.setString(key.toString(), val.toString()));
        return flinkConfig;
    }

    public static void fillUserJarForJobGraph(String jarPath, JobGraph jobGraph) throws UnsupportedEncodingException {
        String addjarPath = URLDecoder.decode(jarPath, Charsets.UTF_8.toString());
        if (addjarPath.length() > 2) {
            addjarPath = addjarPath.substring(1, addjarPath.length() - 1).replace("\"", "");
        }
        List<String> paths = Arrays.asList(addjarPath.split(","));
        paths.forEach(path -> jobGraph.addJar(new Path("file://" + path)));
    }

    public static void fillJobGraphClassPath(JobGraph jobGraph) {
        Map<String, DistributedCache.DistributedCacheEntry> jobCacheFileConfig = jobGraph.getUserArtifacts();
        List<URL> classPath = jobCacheFileConfig.entrySet().stream()
                .filter(tmp -> tmp.getKey().startsWith("class_path"))
                .map(FunctionUtils.uncheckedFunction(tmp -> new URL("file:" + tmp.getValue().filePath)))
                .collect(Collectors.toList());

        jobGraph.getUserArtifacts().clear();
        jobGraph.setClasspaths(classPath);
    }

    public static List<File> getPluginPathToShipFiles(JobGraph jobGraph) {
       List<File> shipFiles = jobGraph.getUserArtifacts()
                .entrySet()
                .stream()
                .filter(tmp -> tmp.getKey().startsWith("class_path"))
                .map(tmp -> new File(tmp.getValue().filePath))
                .collect(Collectors.toList());

        jobGraph.getUserArtifacts().clear();
        return shipFiles;
    }
}
