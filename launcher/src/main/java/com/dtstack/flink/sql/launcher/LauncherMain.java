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

import avro.shaded.com.google.common.collect.Lists;
import com.dtstack.flink.sql.Main;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.PackagedProgram;
import java.io.File;
import java.util.List;
import com.dtstack.flink.sql.ClusterMode;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.table.shaded.org.apache.commons.lang.StringUtils;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.table.shaded.org.apache.commons.lang.BooleanUtils;
import org.apache.flink.configuration.Configuration;
import org.json.JSONObject;

/**
 * Date: 2017/2/20
 * Company: www.dtstack.com
 * @author xuchao
 */

public class   LauncherMain {
    private static final String CORE_JAR = "core.jar";

    private static String SP = File.separator;

    private static String getLocalCoreJarPath(String localSqlRootJar){
        return localSqlRootJar + SP + CORE_JAR;
    }

    private static final int DEFAULT_PARALLELISM = 1;

    public static void main(String[] args) throws Exception {
        LauncherOptionParser optionParser = new LauncherOptionParser(args);
        LauncherOptions launcherOptions = optionParser.getLauncherOptions();
        String mode = launcherOptions.getMode();
        List<String> argList = optionParser.getProgramExeArgList();
        if(mode.equals(ClusterMode.local.name())) {
            String[] localArgs = argList.toArray(new String[argList.size()]);
            Main.main(localArgs);
        } else if (mode.equals(ClusterMode.standalone.name()) || mode.equals(ClusterMode.yarn.name())){
            String pluginRoot = launcherOptions.getLocalSqlPluginPath();
            File jarFile = new File(getLocalCoreJarPath(pluginRoot));
            String[] remoteArgs = argList.toArray(new String[argList.size()]);
            PackagedProgram program = new PackagedProgram(jarFile, Lists.newArrayList(), remoteArgs);
            if(StringUtils.isNotBlank(launcherOptions.getSavePointPath())){
                program.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(launcherOptions.getSavePointPath(), BooleanUtils.toBoolean(launcherOptions.getAllowNonRestoredState())));
            }

            ClusterClient clusterClient = ClusterClientFactory.createClusterClient(launcherOptions);
            clusterClient.run(program, DEFAULT_PARALLELISM);
            clusterClient.shutdown();

            System.exit(0);
        } else if(mode.equals(ClusterMode.yarnPer.name())){
            String pluginRoot = launcherOptions.getLocalSqlPluginPath();
            File jarFile = new File(getLocalCoreJarPath(pluginRoot));
            String[] remoteArgs = argList.toArray(new String[argList.size()]);
            PackagedProgram program = new PackagedProgram(jarFile, Lists.newArrayList(), remoteArgs);
            if(StringUtils.isNotBlank(launcherOptions.getSavePointPath())){
                program.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(launcherOptions.getSavePointPath(), BooleanUtils.toBoolean(launcherOptions.getAllowNonRestoredState())));
            }

            final JobGraph jobGraph = PackagedProgramUtils.createJobGraph(program, new Configuration(), DEFAULT_PARALLELISM);
            ClusterClientFactory.startJob(launcherOptions,jobGraph);

            System.exit(0);
        } else if(mode.equals(ClusterMode.compile.name())){
            String pluginRoot = launcherOptions.getLocalSqlPluginPath();
            File jarFile = new File(getLocalCoreJarPath(pluginRoot));
            String[] remoteArgs = argList.toArray(new String[argList.size()]);
            PackagedProgram program = new PackagedProgram(jarFile, Lists.newArrayList(), remoteArgs);
            if(StringUtils.isNotBlank(launcherOptions.getSavePointPath())){
                program.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(launcherOptions.getSavePointPath(), BooleanUtils.toBoolean(launcherOptions.getAllowNonRestoredState())));
            }
            JSONObject result = new JSONObject();
            try {
                PackagedProgramUtils.createJobGraph(program, new Configuration(), DEFAULT_PARALLELISM);
                result.put("resultStatus","Success");
            }catch (Exception e){
                result.put("resultStatus","Failed");
                result.put("detail",e.getCause());
            }finally {
                System.out.println(result.toString());
            }
            System.exit(0);
        }
    }
}
