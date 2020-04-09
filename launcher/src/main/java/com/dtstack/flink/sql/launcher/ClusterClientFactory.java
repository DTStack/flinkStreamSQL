/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flink.sql.launcher;

import com.dtstack.flink.sql.enums.ClusterMode;
import com.dtstack.flink.sql.option.Options;
import com.dtstack.flink.sql.util.PluginUtil;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.util.LeaderConnectionInfo;
import org.apache.flink.yarn.AbstractYarnClusterDescriptor;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.StringHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.Properties;
import java.util.Iterator;

/**
 * @author sishu.yss
 */
public class ClusterClientFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterClientFactory.class);

    private static final String HA_CLUSTER_ID = "high-availability.cluster-id";

    private static final String HADOOP_CONF = "fs.hdfs.hadoopconf";

    public static ClusterClient createClusterClient(Options launcherOptions) throws Exception {
        String mode = launcherOptions.getMode();
        if (mode.equals(ClusterMode.standalone.name())) {
            return createStandaloneClient(launcherOptions);
        } else if (mode.equals(ClusterMode.yarn.name())) {
            return createYarnSessionClient(launcherOptions);
        }
        throw new IllegalArgumentException("Unsupported cluster client type: ");
    }

    public static ClusterClient createStandaloneClient(Options launcherOptions) throws Exception {
        String flinkConfDir = launcherOptions.getFlinkconf();
        Configuration config = GlobalConfiguration.loadConfiguration(flinkConfDir);

        LOG.info("------------config params-------------------------");
        config.toMap().forEach((key, value) -> LOG.info("{}: {}", key, value));
        LOG.info("-------------------------------------------");

        RestClusterClient clusterClient = new RestClusterClient<>(config, "clusterClient");
        LeaderConnectionInfo connectionInfo = clusterClient.getClusterConnectionInfo();
        InetSocketAddress address = AkkaUtils.getInetSocketAddressFromAkkaURL(connectionInfo.getAddress());
        config.setString(JobManagerOptions.ADDRESS, address.getAddress().getHostName());
        config.setInteger(JobManagerOptions.PORT, address.getPort());
        clusterClient.setDetached(true);
        return clusterClient;
    }

    public static ClusterClient createYarnSessionClient(Options launcherOptions) {
        String flinkConfDir = StringUtils.isEmpty(launcherOptions.getFlinkconf()) ? "" : launcherOptions.getFlinkconf();
        Configuration config = StringUtils.isEmpty(flinkConfDir) ? new Configuration() : GlobalConfiguration.loadConfiguration(flinkConfDir);
        String yarnConfDir = launcherOptions.getYarnconf();

        if (StringUtils.isNotBlank(yarnConfDir)) {
            try {
                config.setString(HADOOP_CONF, yarnConfDir);
                FileSystem.initialize(config);

                YarnConfiguration yarnConf = YarnConfLoader.getYarnConf(yarnConfDir);
                YarnClient yarnClient = YarnClient.createYarnClient();
                yarnClient.init(yarnConf);
                yarnClient.start();
                ApplicationId applicationId;

                String yarnSessionConf = launcherOptions.getYarnSessionConf();
                yarnSessionConf = URLDecoder.decode(yarnSessionConf, Charsets.UTF_8.toString());
                Properties yarnSessionConfProperties = PluginUtil.jsonStrToObject(yarnSessionConf, Properties.class);

                Object yid = yarnSessionConfProperties.get("yid");

                if (null != yid) {
                    applicationId = toApplicationId(yid.toString());
                } else {
                    applicationId = getYarnClusterApplicationId(yarnClient);
                }

                LOG.info("current applicationId = {}", applicationId.toString());

                if (StringUtils.isEmpty(applicationId.toString())) {
                    throw new RuntimeException("No flink session found on yarn cluster.");
                }

                if (config.getString(HA_CLUSTER_ID, null) == null) {
                    config.setString(HA_CLUSTER_ID, applicationId.toString());
                }

                LOG.info("------------config params-------------------------");
                config.toMap().forEach((key, value) -> LOG.info("{}: {}", key, value));
                LOG.info("-------------------------------------------");

                AbstractYarnClusterDescriptor clusterDescriptor = new YarnClusterDescriptor(config, yarnConf, flinkConfDir, yarnClient, false);
                ClusterClient clusterClient = clusterDescriptor.retrieve(applicationId);
                clusterClient.setDetached(true);
                return clusterClient;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new RuntimeException("yarn mode must set param of 'yarnconf'!!!");
        }
    }


    private static ApplicationId getYarnClusterApplicationId(YarnClient yarnClient) throws Exception {
        ApplicationId applicationId = null;

        Set<String> set = new HashSet<>();
        set.add("Apache Flink");
        EnumSet<YarnApplicationState> enumSet = EnumSet.noneOf(YarnApplicationState.class);
        enumSet.add(YarnApplicationState.RUNNING);
        List<ApplicationReport> reportList = yarnClient.getApplications(set, enumSet);

        int maxMemory = -1;
        int maxCores = -1;
        for (ApplicationReport report : reportList) {
            if (!report.getName().startsWith("Flink session")) {
                continue;
            }

            if (!report.getYarnApplicationState().equals(YarnApplicationState.RUNNING)) {
                continue;
            }

            int thisMemory = report.getApplicationResourceUsageReport().getNeededResources().getMemory();
            int thisCores = report.getApplicationResourceUsageReport().getNeededResources().getVirtualCores();
            if (thisMemory > maxMemory || thisMemory == maxMemory && thisCores > maxCores) {
                maxMemory = thisMemory;
                maxCores = thisCores;
                applicationId = report.getApplicationId();
            }

        }

        if (applicationId == null || StringUtils.isEmpty(applicationId.toString())) {
            throw new RuntimeException("No flink session found on yarn cluster.");
        }
        return applicationId;
    }

    private static ApplicationId toApplicationId(String appIdStr) {
        Iterator<String> it = StringHelper._split(appIdStr).iterator();
        if (!"application".equals(it.next())) {
            throw new IllegalArgumentException("Invalid ApplicationId prefix: " + appIdStr + ". The valid ApplicationId should start with prefix " + "application");
        } else {
            try {
                return toApplicationId(it);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid AppAttemptId: " + appIdStr, e);
            }
        }
    }

    private static ApplicationId toApplicationId(Iterator<String> it) throws NumberFormatException {
        return ApplicationId.newInstance(Long.parseLong(it.next()), Integer.parseInt(it.next()));
    }

}