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


import com.dtstack.flink.sql.launcher.entity.JobParamsInfo;
import com.dtstack.flink.sql.launcher.factory.YarnClusterClientFactory;
import com.dtstack.flink.sql.launcher.utils.JobGraphBuildUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.UnsupportedEncodingException;

/**
 * Date: 2020/3/4
 * Company: www.dtstack.com
 * @author maqi
 */
public class YarnSessionClusterExecutor {
    YarnClusterClientFactory yarnClusterClientFactory;
    JobParamsInfo jobParamsInfo;

    public YarnSessionClusterExecutor(JobParamsInfo jobParamsInfo) {
        this.jobParamsInfo = jobParamsInfo;
        yarnClusterClientFactory = new YarnClusterClientFactory();
    }

    public void exec() throws Exception {
        JobGraph jobGraph = JobGraphBuildUtil.buildJobGraph(jobParamsInfo);
        Configuration flinkConfiguration = JobGraphBuildUtil.getFlinkConfiguration(jobParamsInfo.getFlinkConfDir(), jobParamsInfo.getConfProperties());
        ClusterDescriptor clusterDescriptor = yarnClusterClientFactory.createClusterDescriptor(jobParamsInfo.getYarnConfDir(), flinkConfiguration);

        Object yid = jobParamsInfo.getYarnSessionConfProperties().get("yid");
        if (null == yid) {
            throw new RuntimeException("yarnSessionMode yid is required");
        }

        ApplicationId applicationId = ConverterUtils.toApplicationId(yid.toString());
        ClusterClientProvider<ApplicationId> retrieve = clusterDescriptor.retrieve(applicationId);
        ClusterClient<ApplicationId> clusterClient = retrieve.getClusterClient();

        removeSqlPluginAndFillUdfJar(jobGraph, jobParamsInfo.getUdfJar());

        JobExecutionResult jobExecutionResult = ClientUtils.submitJob(clusterClient, jobGraph);
        String jobID = jobExecutionResult.getJobID().toString();
        System.out.println("jobID:" + jobID);

    }

    private void removeSqlPluginAndFillUdfJar(JobGraph jobGraph, String udfJar) throws UnsupportedEncodingException {
        jobGraph.getUserArtifacts().clear();
        jobGraph.getUserJars().clear();
        if (!StringUtils.isEmpty(udfJar)) {
            JobGraphBuildUtil.fillUserJarForJobGraph(udfJar, jobGraph);
        }
    }

}
