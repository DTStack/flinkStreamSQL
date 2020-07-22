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

package com.dtstack.flink.sql.launcher.executor;

import com.dtstack.flink.sql.enums.EPluginLoadMode;
import com.dtstack.flink.sql.launcher.entity.JobParamsInfo;
import com.dtstack.flink.sql.launcher.factory.YarnClusterClientFactory;
import com.dtstack.flink.sql.launcher.utils.JobGraphBuildUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnConfigOptionsInternal;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;


/**
 * Date: 2020/3/4
 * Company: www.dtstack.com
 * @author maqi
 */
public class YarnJobClusterExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(YarnJobClusterExecutor.class);

    private static final String CONFIG_FILE_LOGBACK_NAME = "logback.xml";
    private static final String CONFIG_FILE_LOG4J_NAME = "log4j.properties";
    private static final String DEFAULT_TOTAL_PROCESS_MEMORY = "1024m";

    JobParamsInfo jobParamsInfo;

    public YarnJobClusterExecutor(JobParamsInfo jobParamsInfo) {
        this.jobParamsInfo = jobParamsInfo;
    }

    public void exec() throws Exception {
        JobGraph jobGraph = JobGraphBuildUtil.buildJobGraph(jobParamsInfo);
        if (!StringUtils.isBlank(jobParamsInfo.getUdfJar())) {
            JobGraphBuildUtil.fillUserJarForJobGraph(jobParamsInfo.getUdfJar(), jobGraph);
        }

        Configuration flinkConfiguration = JobGraphBuildUtil.getFlinkConfiguration(jobParamsInfo.getFlinkConfDir(), jobParamsInfo.getConfProperties());
        appendApplicationConfig(flinkConfiguration, jobParamsInfo);

        YarnClusterDescriptor clusterDescriptor = (YarnClusterDescriptor) YarnClusterClientFactory.INSTANCE
                .createClusterDescriptor(jobParamsInfo.getYarnConfDir(), flinkConfiguration);

        List<File> shipFiles = getShipFiles(jobParamsInfo.getFlinkJarPath(), jobParamsInfo.getPluginLoadMode(), jobGraph, clusterDescriptor);

        if (jobParamsInfo.getAddShipFile() != null) {
            List<String> addShipFilesPath = parsePathFromStr(jobParamsInfo.getAddShipFile());
            for (String path : addShipFilesPath) {
                shipFiles.addAll(getShipFiles(path, jobParamsInfo.getPluginLoadMode(), jobGraph, clusterDescriptor));
            }
        }

        clusterDescriptor.addShipFiles(shipFiles);

        ClusterSpecification clusterSpecification = YarnClusterClientFactory.INSTANCE.getClusterSpecification(flinkConfiguration);
        ClusterClientProvider<ApplicationId> applicationIdClusterClientProvider = clusterDescriptor.deployJobCluster(clusterSpecification, jobGraph, true);

        String applicationId = applicationIdClusterClientProvider.getClusterClient().getClusterId().toString();
        String flinkJobId = jobGraph.getJobID().toString();

        LOG.info(String.format("deploy per_job with appId: %s, jobId: %s", applicationId, flinkJobId));
    }

    private void appendApplicationConfig(Configuration flinkConfig, JobParamsInfo jobParamsInfo) {
        if (!StringUtils.isEmpty(jobParamsInfo.getName())) {
            flinkConfig.setString(YarnConfigOptions.APPLICATION_NAME, jobParamsInfo.getName());
        }

        if (!StringUtils.isEmpty(jobParamsInfo.getQueue())) {
            flinkConfig.setString(YarnConfigOptions.APPLICATION_QUEUE, jobParamsInfo.getQueue());
        }

        if (!StringUtils.isEmpty(jobParamsInfo.getFlinkConfDir())) {
            discoverLogConfigFile(jobParamsInfo.getFlinkConfDir()).ifPresent(file ->
                    flinkConfig.setString(YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE, file.getPath()));
        }

        if (!flinkConfig.contains(TaskManagerOptions.TOTAL_PROCESS_MEMORY)) {
            flinkConfig.setString(TaskManagerOptions.TOTAL_PROCESS_MEMORY.key(), DEFAULT_TOTAL_PROCESS_MEMORY);
        }
    }

    protected List<File> getShipFiles(String flinkJarPath, String pluginLoadMode, JobGraph jobGraph, YarnClusterDescriptor clusterDescriptor)
            throws MalformedURLException {

        List<File> shipFiles = new ArrayList<>();
        dealFlinkLibJar(flinkJarPath, clusterDescriptor, shipFiles);
        dealUserJarByPluginLoadMode(pluginLoadMode, jobGraph, shipFiles);
        return shipFiles;
    }

    private void dealFlinkLibJar(String flinkJarPath, YarnClusterDescriptor clusterDescriptor, List<File> shipFiles) throws MalformedURLException {
        if (StringUtils.isEmpty(flinkJarPath) || !new File(flinkJarPath).exists()) {
            throw new RuntimeException("path " + flinkJarPath + " is not exist");
        }
        File[] jars = new File(flinkJarPath).listFiles();

        if (jars == null || jars.length == 0) {
            throw new RuntimeException(flinkJarPath + " no file exist !");
        }

        for (File file : jars) {
            if (file.toURI().toURL().toString().contains("flink-dist")) {
                clusterDescriptor.setLocalJarPath(new Path(file.toURI().toURL().toString()));
            } else {
                shipFiles.add(file);
            }
        }
    }

    private void dealUserJarByPluginLoadMode(String pluginLoadMode, JobGraph jobGraph, List<File> shipFiles) throws MalformedURLException {
        // classpath , all node need contain plugin jar
        if (StringUtils.equalsIgnoreCase(pluginLoadMode, EPluginLoadMode.CLASSPATH.name())) {
            JobGraphBuildUtil.fillJobGraphClassPath(jobGraph);
        } else if (StringUtils.equalsIgnoreCase(pluginLoadMode, EPluginLoadMode.SHIPFILE.name())) {
            List<File> pluginPaths = JobGraphBuildUtil.getPluginPathToShipFiles(jobGraph);
            shipFiles.addAll(pluginPaths);
        } else {
            throw new IllegalArgumentException("Unsupported plugin loading mode " + pluginLoadMode
                    + " Currently only classpath and shipfile are supported.");
        }
    }


    private Optional<File> discoverLogConfigFile(final String configurationDirectory) {
        Optional<File> logConfigFile = Optional.empty();

        final File log4jFile = new File(configurationDirectory + File.separator + CONFIG_FILE_LOG4J_NAME);
        if (log4jFile.exists()) {
            logConfigFile = Optional.of(log4jFile);
        }

        final File logbackFile = new File(configurationDirectory + File.separator + CONFIG_FILE_LOGBACK_NAME);
        if (logbackFile.exists()) {
            if (logConfigFile.isPresent()) {
                LOG.warn("The configuration directory ('" + configurationDirectory + "') already contains a LOG4J config file." +
                        "If you want to use logback, then please delete or rename the log configuration file.");
            } else {
                logConfigFile = Optional.of(logbackFile);
            }
        }
        return logConfigFile;
    }

    private static List<String> parsePathFromStr(String pathStr) {
        if (pathStr.length() > 2) {
            pathStr = pathStr.substring(1, pathStr.length() - 1).replace("\"", "");
        }

        return Arrays.asList(pathStr.split(","));
    }
}
