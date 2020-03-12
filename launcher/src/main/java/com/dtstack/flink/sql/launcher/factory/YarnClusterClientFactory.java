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

package com.dtstack.flink.sql.launcher.factory;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.function.FunctionUtils;
import org.apache.flink.yarn.YarnClientYarnClusterInformationRetriever;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;


/**
 * Date: 2020/3/4
 * Company: www.dtstack.com
 * @author maqi
 */
public class YarnClusterClientFactory extends AbstractClusterClientFactory {
    private static final String XML_FILE_EXTENSION = "xml";
    @Override
    public ClusterDescriptor createClusterDescriptor(String yarnConfDir, Configuration flinkConfig) {

        if (StringUtils.isNotBlank(yarnConfDir)) {
            try {
                flinkConfig.setString(ConfigConstants.PATH_HADOOP_CONFIG, yarnConfDir);
                FileSystem.initialize(flinkConfig, null);

                YarnConfiguration yarnConf = getYarnConf(yarnConfDir);
                YarnClient yarnClient = YarnClient.createYarnClient();
                yarnClient.init(yarnConf);
                yarnClient.start();

                YarnClusterDescriptor clusterDescriptor = new YarnClusterDescriptor(
                        flinkConfig,
                        yarnConf,
                        yarnClient,
                        YarnClientYarnClusterInformationRetriever.create(yarnClient),
                        false);

                return clusterDescriptor;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new RuntimeException("yarn mode must set param of 'yarnconf'!!!");
        }
    }

    private YarnConfiguration getYarnConf(String yarnConfDir) throws IOException {
        YarnConfiguration yarnConf = new YarnConfiguration();
        FileUtils.listFilesInDirectory(new File(yarnConfDir).toPath(), this::isXmlFile)
                .stream()
                .map(FunctionUtils.uncheckedFunction(FileUtils::toURL))
                .forEach(yarnConf::addResource);

        haYarnConf(yarnConf);
        return yarnConf;
    }

    /**
     * deal yarn HA conf
     */
    private org.apache.hadoop.conf.Configuration haYarnConf(org.apache.hadoop.conf.Configuration yarnConf) {
        Iterator<Map.Entry<String, String>> iterator = yarnConf.iterator();
        iterator.forEachRemaining((Map.Entry<String, String> entry) -> {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key.startsWith("yarn.resourcemanager.hostname.")) {
                String rm = key.substring("yarn.resourcemanager.hostname.".length());
                String addressKey = "yarn.resourcemanager.address." + rm;
                if (yarnConf.get(addressKey) == null) {
                    yarnConf.set(addressKey, value + ":" + YarnConfiguration.DEFAULT_RM_PORT);
                }
            }
        });

        return yarnConf;
    }

    private boolean isXmlFile(java.nio.file.Path file) {
        return XML_FILE_EXTENSION.equals(org.apache.flink.shaded.guava18.com.google.common.io.Files.getFileExtension(file.toString()));
    }
}
