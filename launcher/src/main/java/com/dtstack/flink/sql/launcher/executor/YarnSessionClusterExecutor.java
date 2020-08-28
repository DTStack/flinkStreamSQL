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


/**
 * Date: 2020/3/4
 * Company: www.dtstack.com
 * @author maqi
 */
public class YarnSessionClusterExecutor {
    JobParamsInfo jobParamsInfo;

    public YarnSessionClusterExecutor(JobParamsInfo jobParamsInfo) {
        this.jobParamsInfo = jobParamsInfo;
    }

    public void exec() throws Exception {
        JobGraph jobGraph = JobGraphBuildUtil.buildJobGraph(jobParamsInfo);
        Configuration flinkConfiguration = JobGraphBuildUtil.getFlinkConfiguration(jobParamsInfo.getFlinkConfDir(), jobParamsInfo.getConfProperties());
        ClusterDescriptor clusterDescriptor = YarnClusterClientFactory.INSTANCE.createClusterDescriptor(jobParamsInfo.getYarnConfDir(), flinkConfiguration);

        Object yid = jobParamsInfo.getYarnSessionConfProperties().get("yid");
        if (null == yid) {
            throw new RuntimeException("yarnSessionMode yid is required");
        }

        ApplicationId applicationId = ConverterUtils.toApplicationId(yid.toString());
        ClusterClientProvider<ApplicationId> retrieve = clusterDescriptor.retrieve(applicationId);
        ClusterClient<ApplicationId> clusterClient = retrieve.getClusterClient();

        if (StringUtils.equalsIgnoreCase(jobParamsInfo.getPluginLoadMode(), EPluginLoadMode.SHIPFILE.name())) {
            jobGraph.getUserArtifacts().clear();
        } else {
            JobGraphBuildUtil.fillJobGraphClassPath(jobGraph);
        }

        if (!StringUtils.isEmpty(jobParamsInfo.getUdfJar())) {
            JobGraphBuildUtil.fillUserJarForJobGraph(jobParamsInfo.getUdfJar(), jobGraph);
        }

        JobExecutionResult jobExecutionResult = ClientUtils.submitJob(clusterClient, jobGraph);
        String jobId = jobExecutionResult.getJobID().toString();
        System.out.println("jobID:" + jobId);

    }


}
