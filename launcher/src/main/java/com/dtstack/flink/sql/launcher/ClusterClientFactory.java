/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flink.sql.launcher;

import com.dtstack.flink.sql.util.PluginUtil;
import com.dtstack.flink.yarn.JobParameter;
import com.dtstack.flink.yarn.YarnClusterConfiguration;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.calcite.shaded.com.google.common.base.Strings;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.StandaloneClusterClient;
import org.apache.flink.configuration.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.util.LeaderConnectionInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.yarn.AbstractYarnClusterDescriptor;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import java.io.File;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.util.*;
import com.dtstack.flink.sql.ClusterMode;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

/**
 * The Factory of ClusterClient
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class ClusterClientFactory {
    private static final Logger LOG = LoggerFactory.getLogger(ClusterClientFactory.class);
    private static final ObjectMapper objMapper = new ObjectMapper();

    public static ClusterClient createClusterClient(LauncherOptions launcherOptions) throws Exception {
        String mode = launcherOptions.getMode();
        if(mode.equals(ClusterMode.standalone.name())) {
            return createStandaloneClient(launcherOptions);
        } else if(mode.equals(ClusterMode.yarn.name())) {
            return createYarnClient(launcherOptions);
        }
        throw new IllegalArgumentException("Unsupported cluster client type: ");
    }

    public static ClusterClient createStandaloneClient(LauncherOptions launcherOptions) throws Exception {
        String flinkConfDir = launcherOptions.getFlinkconf();
        Configuration config = GlobalConfiguration.loadConfiguration(flinkConfDir);
        StandaloneClusterClient clusterClient = new StandaloneClusterClient(config);
        LeaderConnectionInfo connectionInfo = clusterClient.getClusterConnectionInfo();
        InetSocketAddress address = AkkaUtils.getInetSocketAddressFromAkkaURL(connectionInfo.getAddress());
        config.setString(JobManagerOptions.ADDRESS, address.getAddress().getHostName());
        config.setInteger(JobManagerOptions.PORT, address.getPort());
        clusterClient.setDetached(true);
        return clusterClient;
    }

    public static ClusterClient createYarnClient(LauncherOptions launcherOptions) {
        String flinkConfDir = launcherOptions.getFlinkconf();
        Configuration flinkConf = GlobalConfiguration.loadConfiguration(flinkConfDir);
        String yarnConfDir = launcherOptions.getYarnconf();
        YarnConfiguration yarnConf;
        if(StringUtils.isNotBlank(yarnConfDir)) {
            try {
                flinkConf.setString(ConfigConstants.PATH_HADOOP_CONFIG, yarnConfDir);
                FileSystem.initialize(flinkConf);

                File dir = new File(yarnConfDir);
                if(dir.exists() && dir.isDirectory()) {
                    yarnConf = loadYarnConfiguration(yarnConfDir);

                    YarnClient yarnClient = YarnClient.createYarnClient();
                    haYarnConf(yarnConf);
                    yarnClient.init(yarnConf);
                    yarnClient.start();

                    ApplicationId applicationId = getYarnClusterApplicationId(yarnClient);
                    System.out.println("applicationId="+applicationId.toString());
                    LOG.info("applicationId= {}", applicationId.toString());
                    AbstractYarnClusterDescriptor clusterDescriptor = new YarnClusterDescriptor(
                                flinkConf, yarnConf, ".", yarnClient, false);
                    ClusterClient clusterClient = clusterDescriptor.retrieve(applicationId);

                    System.out.println("applicationId="+applicationId.toString()+" has retrieve!");
                    LOG.info("applicationId= {}  has retrieve!", applicationId.toString());

                    clusterClient.setDetached(true);
                    yarnClient.stop();
                    return clusterClient;
                }
            } catch(Exception e) {
                LOG.error("createYarnClient ERROR:{}");
                throw new RuntimeException(e);
            }
        }
        throw new UnsupportedOperationException("Haven't been developed yet!");
    }

    public static void startJob(LauncherOptions launcherOptions, JobGraph jobGraph) {
        String flinkConfDir = launcherOptions.getFlinkconf();
        Configuration flinkConf = GlobalConfiguration.loadConfiguration(flinkConfDir);
        String yarnConfDir = launcherOptions.getYarnconf();
        YarnConfiguration yarnConf;
        if(StringUtils.isNotBlank(yarnConfDir)) {
            try {
                flinkConf.setString(ConfigConstants.PATH_HADOOP_CONFIG, yarnConfDir);
                FileSystem.initialize(flinkConf);

                File dir = new File(yarnConfDir);
                if(dir.exists() && dir.isDirectory()) {
                    yarnConf = loadYarnConfiguration(yarnConfDir);

                    YarnClient yarnClient = YarnClient.createYarnClient();
                    haYarnConf(yarnConf);
                    yarnClient.init(yarnConf);
                    yarnClient.start();

                    String confProp = launcherOptions.getConfProp();
                    confProp = URLDecoder.decode(confProp, Charsets.UTF_8.toString());
                    LOG.info("confProp= {}", confProp);
                    Properties confProperties = PluginUtil.jsonStrToObject(confProp, Properties.class);

                    String addJarListStr = launcherOptions.getAddjar();
                    List<String> addJarFileList = Lists.newArrayList();
                    if(!Strings.isNullOrEmpty(addJarListStr)){
                        addJarListStr = URLDecoder.decode(addJarListStr, Charsets.UTF_8.name());
                        addJarFileList = objMapper.readValue(addJarListStr, List.class);
                    }

                    YarnClusterConfiguration clusterConf = getYarnClusterConfiguration(flinkConf,yarnConf,flinkConfDir,addJarFileList);
                    JobParameter jobParam = new JobParameter(confProperties);

                    AbstractYarnClusterDescriptor clusterDescriptor = createDescriptor(jobParam,clusterConf,flinkConf, yarnConf, flinkConfDir, yarnClient,launcherOptions.getName());
                    ClusterSpecification clusterSpecification = createClusterSpecification(jobParam);
                    ClusterClient client = clusterDescriptor.deployJobCluster(clusterSpecification,jobGraph,true);

                    System.out.println("applicationId="+client.getClusterId());
                    LOG.info("applicationId= {}  has deployed!", client.getClusterId());

                    yarnClient.stop();
                    client.shutdown();
                }
            } catch(Exception e) {
                LOG.error("createYarnClient ERROR:{}");
                throw new RuntimeException(e);
            }
        }
    }

    private static YarnConfiguration loadYarnConfiguration(String yarnConfDir)
    {
        org.apache.hadoop.conf.Configuration hadoopConf = new org.apache.hadoop.conf.Configuration();
        hadoopConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        Stream.of("yarn-site.xml", "core-site.xml", "hdfs-site.xml").forEach(file -> {
            File site = new File(requireNonNull(yarnConfDir, "ENV HADOOP_CONF_DIR is not setting"), file);
            if (site.exists() && site.isFile()) {
                hadoopConf.addResource(new org.apache.hadoop.fs.Path(site.toURI()));
            }
            else {
                throw new RuntimeException(site + " not exists");
            }
        });

        return new YarnConfiguration(hadoopConf);
    }

    public static YarnClusterConfiguration getYarnClusterConfiguration(Configuration flinkConf,YarnConfiguration yarnConf,String flinkConfDir)
    {
        return getYarnClusterConfiguration(flinkConf,yarnConf,flinkConfDir,null);
    }

    public static YarnClusterConfiguration getYarnClusterConfiguration(Configuration flinkConf,YarnConfiguration yarnConf,String flinkConfDir,List<String> udfJarList)
    {
        Path flinkJar = new Path(getFlinkJarFile(flinkConfDir).toURI());

        final Set<Path> resourcesToLocalize = Stream
                .of("../lib")
                .map(x -> new Path(flinkConfDir, x))
                .collect(Collectors.toSet());
        if(!udfJarList.isEmpty()){
            for (String udfJar:udfJarList){
                resourcesToLocalize.add(new Path(udfJar));
            }
        }

        return new YarnClusterConfiguration(
                flinkConf,
                yarnConf,
                "",
                flinkJar,
                resourcesToLocalize);
    }

    private static final String FLINK_DIST = "flink-dist";
    private static File getFlinkJarFile(String flinkConfDir)
    {
        String errorMessage = "error not search " + FLINK_DIST + "*.jar";
        File[] files = requireNonNull(new File(flinkConfDir, "/../lib").listFiles(), errorMessage);
        Optional<File> file = Arrays.stream(files)
                .filter(f -> f.getName().startsWith(FLINK_DIST)).findFirst();
        return file.orElseThrow(() -> new IllegalArgumentException(errorMessage));
    }

    private static ApplicationId getYarnClusterApplicationId(YarnClient yarnClient) throws Exception{
        ApplicationId applicationId = null;

        Set<String> set = new HashSet<>();
        set.add("Apache Flink");
        EnumSet<YarnApplicationState> enumSet = EnumSet.noneOf(YarnApplicationState.class);
        enumSet.add(YarnApplicationState.RUNNING);
        List<ApplicationReport> reportList = yarnClient.getApplications(set, enumSet);

        int maxMemory = -1;
        int maxCores = -1;
        for(ApplicationReport report : reportList) {
            if(!report.getName().startsWith("Flink session")){
                continue;
            }

            if(!report.getYarnApplicationState().equals(YarnApplicationState.RUNNING)) {
                continue;
            }

            int thisMemory = report.getApplicationResourceUsageReport().getNeededResources().getMemory();
            int thisCores = report.getApplicationResourceUsageReport().getNeededResources().getVirtualCores();
            if(thisMemory > maxMemory || thisMemory == maxMemory && thisCores > maxCores) {
                maxMemory = thisMemory;
                maxCores = thisCores;
                applicationId = report.getApplicationId();
            }

        }

        if(StringUtils.isEmpty(applicationId.toString())) {
            throw new RuntimeException("No flink session found on yarn cluster.");
        }
        return applicationId;
    }

    /**
     * 处理yarn HA的配置项
     */
    private static org.apache.hadoop.conf.Configuration haYarnConf(org.apache.hadoop.conf.Configuration yarnConf) {
        Iterator<Map.Entry<String, String>> iterator = yarnConf.iterator();
        while(iterator.hasNext()) {
            Map.Entry<String,String> entry = iterator.next();
            String key = entry.getKey();
            String value = entry.getValue();
            if(key.startsWith("yarn.resourcemanager.hostname.")) {
                String rm = key.substring("yarn.resourcemanager.hostname.".length());
                String addressKey = "yarn.resourcemanager.address." + rm;
                if(yarnConf.get(addressKey) == null) {
                    yarnConf.set(addressKey, value + ":" + YarnConfiguration.DEFAULT_RM_PORT);
                }
            }
        }
        return yarnConf;
    }

    private static AbstractYarnClusterDescriptor createDescriptor(
            JobParameter jobParam,
            YarnClusterConfiguration clusterConf,
            Configuration configuration,
            YarnConfiguration yarnConfiguration,
            String configurationDirectory,
            YarnClient yarnClient,
            String jobName) {

        AbstractYarnClusterDescriptor yarnClusterDescriptor = new YarnClusterDescriptor(
                configuration,
                yarnConfiguration,
                configurationDirectory,
                yarnClient,
                false);

        if (clusterConf.flinkJar() != null) {
            yarnClusterDescriptor.setLocalJarPath(clusterConf.flinkJar());
        }

        List<File> shipFiles = new ArrayList<>();
        // path to directory to ship
        if(clusterConf.resourcesToLocalize() != null) {
            for (Path shipPath : clusterConf.resourcesToLocalize()) {
                shipFiles.add(new File(shipPath.toString()));
            }
        }
        yarnClusterDescriptor.addShipFiles(shipFiles);

        // queue
        if (jobParam.getQueue() != null) {
            yarnClusterDescriptor.setQueue(jobParam.getQueue());
        }

        if (jobName != null) {
            yarnClusterDescriptor.setName(jobName);
        }

        yarnClusterDescriptor.setDetachedMode(true);
        return yarnClusterDescriptor;
    }

    private static ClusterSpecification createClusterSpecification(JobParameter appConf) {
        return new ClusterSpecification.ClusterSpecificationBuilder()
                .setMasterMemoryMB(appConf.getJobManagerMemoryMb())
                .setTaskManagerMemoryMB(appConf.getTaskManagerMemoryMb())
                .setNumberTaskManagers(appConf.getTaskManagerCount())
                .setSlotsPerTaskManager(appConf.getTaskManagerSlots())
                .createClusterSpecification();
    }
}
