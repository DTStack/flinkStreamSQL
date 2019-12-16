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

package com.dtstack.flink.sql.launcher.perjob;

import com.dtstack.flink.sql.enums.EPluginLoadMode;
import com.dtstack.flink.sql.launcher.YarnConfLoader;
import com.dtstack.flink.sql.option.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.configuration.Configuration;
import com.google.common.base.Strings;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.yarn.AbstractYarnClusterDescriptor;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Reason:
 * Date: 2018/11/16
 * Company: www.dtstack.com
 * @author xuchao
 */

public class PerJobClusterClientBuilder {
    private YarnClient yarnClient;

    private YarnConfiguration yarnConf;

    public void init(String yarnConfDir){
        if(Strings.isNullOrEmpty(yarnConfDir)) {
            throw new RuntimeException("parameters of yarn is required");
        }

        yarnConf = YarnConfLoader.getYarnConf(yarnConfDir);
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(yarnConf);
        yarnClient.start();

        System.out.println("----init yarn success ----");
    }

    public AbstractYarnClusterDescriptor createPerJobClusterDescriptor(Properties confProp, String flinkJarPath, Options launcherOptions, JobGraph jobGraph) throws MalformedURLException {
        Configuration newConf = new Configuration();
        confProp.forEach((key, val) -> newConf.setString(key.toString(), val.toString()));

        AbstractYarnClusterDescriptor clusterDescriptor = getClusterDescriptor(newConf, yarnConf, ".");

        if (StringUtils.isNotBlank(flinkJarPath)) {
            if (!new File(flinkJarPath).exists()) {
                throw new RuntimeException("The Flink jar path is not exist");
            }
        }

        List<File> shipFiles = new ArrayList<>();
        if (flinkJarPath != null) {
            File[] jars = new File(flinkJarPath).listFiles();
            for (File file : jars) {
                if (file.toURI().toURL().toString().contains("flink-dist")) {
                    clusterDescriptor.setLocalJarPath(new Path(file.toURI().toURL().toString()));
                } else {
                    shipFiles.add(file);
                }
            }
        } else {
            throw new RuntimeException("The Flink jar path is null");
        }
        // classpath , all node need contain plugin jar
        String pluginLoadMode = launcherOptions.getPluginLoadMode();
        if (StringUtils.equalsIgnoreCase(pluginLoadMode, EPluginLoadMode.CLASSPATH.name())) {
            fillJobGraphClassPath(jobGraph);
        } else if (StringUtils.equalsIgnoreCase(pluginLoadMode, EPluginLoadMode.SHIPFILE.name())) {
            List<File> pluginPaths = getPluginPathToShipFiles(jobGraph);
            shipFiles.addAll(pluginPaths);
        } else {
            throw new IllegalArgumentException("Unsupported plugin loading mode " + pluginLoadMode
                    + " Currently only classpath and shipfile are supported.");
        }

        clusterDescriptor.addShipFiles(shipFiles);
        clusterDescriptor.setName(launcherOptions.getName());
        String queue = launcherOptions.getQueue();
        if (!Strings.isNullOrEmpty(queue)) {
            clusterDescriptor.setQueue(queue);
        }
        return clusterDescriptor;
    }

    private static void fillJobGraphClassPath(JobGraph jobGraph) throws MalformedURLException {
        Map<String, DistributedCache.DistributedCacheEntry> jobCacheFileConfig = jobGraph.getUserArtifacts();
        for(Map.Entry<String,  DistributedCache.DistributedCacheEntry> tmp : jobCacheFileConfig.entrySet()){
            if(tmp.getKey().startsWith("class_path")){
                jobGraph.getClasspaths().add(new URL("file:" + tmp.getValue().filePath));
            }
        }
    }

    private List<File> getPluginPathToShipFiles(JobGraph jobGraph) {
        List<File> shipFiles = new ArrayList<>();
        Map<String, DistributedCache.DistributedCacheEntry> jobCacheFileConfig = jobGraph.getUserArtifacts();
        for(Map.Entry<String,  DistributedCache.DistributedCacheEntry> tmp : jobCacheFileConfig.entrySet()){
            if(tmp.getKey().startsWith("class_path")){
                shipFiles.add(new File(tmp.getValue().filePath));
            }
        }
        return shipFiles;
    }

    private AbstractYarnClusterDescriptor getClusterDescriptor(
            Configuration configuration,
            YarnConfiguration yarnConfiguration,
            String configurationDirectory) {
        return new YarnClusterDescriptor(
                configuration,
                yarnConfiguration,
                configurationDirectory,
                yarnClient,
                false);
    }
}
