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

import org.apache.commons.lang.StringUtils;
import org.apache.flink.client.deployment.ClusterRetrieveException;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.StandaloneClusterDescriptor;
import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.StandaloneClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.highavailability.zookeeper.ZooKeeperHaServices;
import org.apache.flink.runtime.util.LeaderConnectionInfo;
import org.apache.flink.yarn.AbstractYarnClusterDescriptor;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import java.io.File;
import java.io.FilenameFilter;
import java.net.InetSocketAddress;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import com.dtstack.flink.sql.ClusterMode;

/**
 * The Factory of ClusterClient
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class ClusterClientFactory {

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

        StandaloneClusterDescriptor standaloneClusterDescriptor = new StandaloneClusterDescriptor(config);
        RestClusterClient clusterClient = standaloneClusterDescriptor.retrieve(StandaloneClusterId.getInstance());

        LeaderConnectionInfo connectionInfo = clusterClient.getClusterConnectionInfo();
        InetSocketAddress address = AkkaUtils.getInetSocketAddressFromAkkaURL(connectionInfo.getAddress());
        config.setString(JobManagerOptions.ADDRESS, address.getAddress().getHostName());
        config.setInteger(JobManagerOptions.PORT, address.getPort());
        clusterClient.setDetached(true);
        return clusterClient;
    }

    public static ClusterClient createYarnClient(LauncherOptions launcherOptions) {
        String flinkConfDir = launcherOptions.getFlinkconf();
        Configuration config = GlobalConfiguration.loadConfiguration(flinkConfDir);
        String yarnConfDir = launcherOptions.getYarnconf();
        YarnConfiguration yarnConf = new YarnConfiguration();
        if(StringUtils.isNotBlank(yarnConfDir)) {
            try {

                config.setString(ConfigConstants.PATH_HADOOP_CONFIG, yarnConfDir);
                FileSystem.initialize(config);

                File dir = new File(yarnConfDir);
                if(dir.exists() && dir.isDirectory()) {
                    File[] xmlFileList = new File(yarnConfDir).listFiles(new FilenameFilter() {
                        @Override
                        public boolean accept(File dir, String name) {
                            if(name.endsWith(".xml")){
                                return true;
                            }
                            return false;
                        }
                    });

                    if(xmlFileList != null) {
                        for(File xmlFile : xmlFileList) {
                            yarnConf.addResource(xmlFile.toURI().toURL());
                        }
                    }

                    YarnClient yarnClient = YarnClient.createYarnClient();
                    haYarnConf(yarnConf);
                    yarnClient.init(yarnConf);
                    yarnClient.start();
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


                    AbstractYarnClusterDescriptor clusterDescriptor = new YarnClusterDescriptor(config, yarnConf, ".", yarnClient, false);
                    ClusterClient clusterClient = clusterDescriptor.retrieve(applicationId);
                    clusterClient.setDetached(true);
                    return clusterClient;
                }
            } catch(Exception e) {
                throw new RuntimeException(e);
            }
        }



        throw new UnsupportedOperationException("Haven't been developed yet!");
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

    private static org.apache.hadoop.conf.Configuration getYarnConf(String yarnConfDir) {
        org.apache.hadoop.conf.Configuration yarnConf = new YarnConfiguration();
        try {

            File dir = new File(yarnConfDir);
            if(dir.exists() && dir.isDirectory()) {
                File[] xmlFileList = new File(yarnConfDir).listFiles(new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                        if(name.endsWith(".xml")){
                            return true;
                        }
                        return false;
                    }
                });
                if(xmlFileList != null) {
                    for(File xmlFile : xmlFileList) {
                        yarnConf.addResource(xmlFile.toURI().toURL());
                    }
                }
            }
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
        return yarnConf;
    }

}
