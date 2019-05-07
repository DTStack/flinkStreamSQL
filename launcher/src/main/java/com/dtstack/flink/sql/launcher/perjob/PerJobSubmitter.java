/*
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

import com.dtstack.flink.sql.launcher.LauncherOptions;
import com.dtstack.flink.sql.util.PluginUtil;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.shaded.guava18.com.google.common.base.Strings;
import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;
import org.apache.flink.yarn.AbstractYarnClusterDescriptor;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * per job mode submitter
 * Date: 2018/11/17
 * Company: www.dtstack.com
 * @author xuchao
 */

public class PerJobSubmitter {

    private static final Logger LOG = LoggerFactory.getLogger(PerJobSubmitter.class);

    public static String submit(LauncherOptions launcherOptions, JobGraph jobGraph) throws Exception {

        fillJobGraphClassPath(jobGraph);

        String confProp = launcherOptions.getConfProp();
        confProp = URLDecoder.decode(confProp, Charsets.UTF_8.toString());
        Properties confProperties = PluginUtil.jsonStrToObject(confProp, Properties.class);
        ClusterSpecification clusterSpecification = FLinkPerJobResourceUtil.createClusterSpecification(confProperties);

        PerJobClusterClientBuilder perJobClusterClientBuilder = new PerJobClusterClientBuilder();
        perJobClusterClientBuilder.init(launcherOptions.getYarnconf());

        String flinkJarPath = launcherOptions.getFlinkJarPath();

        AbstractYarnClusterDescriptor yarnClusterDescriptor = perJobClusterClientBuilder.createPerJobClusterDescriptor(confProperties, flinkJarPath, launcherOptions.getQueue());
        ClusterClient<ApplicationId> clusterClient = yarnClusterDescriptor.deployJobCluster(clusterSpecification, jobGraph,true);

        String applicationId = clusterClient.getClusterId().toString();
        String flinkJobId = jobGraph.getJobID().toString();

        String tips = String.format("deploy per_job with appId: %s, jobId: %s", applicationId, flinkJobId);
        System.out.println(tips);
        LOG.info(tips);

        return applicationId;
    }

    private static void fillJobGraphClassPath(JobGraph jobGraph) throws MalformedURLException {
        Map<String, String> jobCacheFileConfig = jobGraph.getJobConfiguration().toMap();
        Set<String> classPathKeySet = Sets.newHashSet();

        for(Map.Entry<String, String> tmp : jobCacheFileConfig.entrySet()){
            if(Strings.isNullOrEmpty(tmp.getValue())){
                continue;
            }

            if(tmp.getValue().startsWith("class_path")){
                //DISTRIBUTED_CACHE_FILE_NAME_1
                //DISTRIBUTED_CACHE_FILE_PATH_1
                String key = tmp.getKey();
                String[] array = key.split("_");
                if(array.length < 5){
                    continue;
                }

                array[3] = "PATH";
                classPathKeySet.add(StringUtils.join(array, "_"));
            }
        }

        for(String key : classPathKeySet){
            String pathStr = jobCacheFileConfig.get(key);
            jobGraph.getClasspaths().add(new URL("file:" + pathStr));
        }
    }
}
