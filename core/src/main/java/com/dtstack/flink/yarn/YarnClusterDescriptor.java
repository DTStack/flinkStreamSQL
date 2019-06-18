/*
 * Copyright (C) 2018 The Sylph Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flink.yarn;

import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.yarn.*;
import org.apache.flink.yarn.entrypoint.YarnSessionClusterEntrypoint;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.yarn.api.records.YarnApplicationState.NEW;

public class YarnClusterDescriptor
        extends AbstractYarnClusterDescriptor
{
    private static final String APPLICATION_TYPE = "58_FLINK";
    private static final Logger LOG = LoggerFactory.getLogger(YarnClusterDescriptor.class);
    private static final int MAX_ATTEMPT = 1;
    private static final long DEPLOY_TIMEOUT_MS = 600 * 1000;
    private static final long RETRY_DELAY_MS = 250;
    private static final ScheduledExecutorService YARN_POLL_EXECUTOR = Executors.newSingleThreadScheduledExecutor();

    private final YarnClusterConfiguration clusterConf;
    private final YarnClient yarnClient;
    private final JobParameter appConf;
    private final Path homedir;
    private final ApplicationId yarnAppId;
    private final String jobName;
    private final Iterable<Path> userProvidedJars;
    private Path flinkJar;

    public YarnClusterDescriptor(
            final YarnClusterConfiguration clusterConf,
            final YarnClient yarnClient,
            final JobParameter appConf,
            ApplicationId yarnAppId,
            String jobName,
            Iterable<Path> userProvidedJars)
    {
        super(clusterConf.flinkConfiguration(), clusterConf.yarnConf(), clusterConf.appRootDir(), yarnClient, false);
        this.jobName = jobName;
        this.clusterConf = clusterConf;
        this.yarnClient = yarnClient;
        this.appConf = appConf;
        this.yarnAppId = yarnAppId;
        this.userProvidedJars = userProvidedJars;
        this.homedir = new Path(clusterConf.appRootDir(), yarnAppId.toString());
    }

    @Override
    protected String getYarnSessionClusterEntrypoint()
    {
        return YarnSessionClusterEntrypoint.class.getName();
    }

    /**
     * 提交到yarn时 任务启动入口类
     */
    @Override
    protected String getYarnJobClusterEntrypoint()
    {
        return YarnSessionClusterEntrypoint.class.getName();
    }

    @Override
    protected ClusterClient<ApplicationId> createYarnClusterClient(AbstractYarnClusterDescriptor descriptor, int numberTaskManagers, int slotsPerTaskManager, ApplicationReport report, Configuration flinkConfiguration, boolean perJobCluster)
            throws Exception
    {
        return new RestClusterClient<>(
                flinkConfiguration,
                report.getApplicationId());
    }

    @Override
    public YarnClient getYarnClient()
    {
        return this.yarnClient;
    }

    public RestClusterClient deploy()
    {
        ApplicationSubmissionContext context = Records.newRecord(ApplicationSubmissionContext.class);
        context.setApplicationId(yarnAppId);
        try {
            ApplicationReport report = startAppMaster(context);

            Configuration conf = getFlinkConfiguration();
            conf.setString(JobManagerOptions.ADDRESS.key(), report.getHost());
            conf.setInteger(JobManagerOptions.PORT.key(), report.getRpcPort());

            /*return new RestClusterClient(this,
                    appConf.getTaskManagerCount(),
                    appConf.getTaskManagerSlots(),
                    report, conf, false);*/
            return new RestClusterClient<>(
                    conf,
                    report.getApplicationId());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ApplicationReport startAppMaster(ApplicationSubmissionContext appContext)
            throws Exception
    {
        ApplicationId appId = appContext.getApplicationId();
        appContext.setMaxAppAttempts(MAX_ATTEMPT);

        Map<String, LocalResource> localResources = new HashMap<>();
        Set<Path> shippedPaths = new HashSet<>();
        collectLocalResources(localResources, shippedPaths);

        final ContainerLaunchContext amContainer = setupApplicationMasterContainer(
                getYarnJobClusterEntrypoint(),
                false,
                true,
                false,
                appConf.getJobManagerMemoryMb()
        );

        amContainer.setLocalResources(localResources);

        final String classPath = String.join(File.pathSeparator, localResources.keySet());

        final String shippedFiles = shippedPaths.stream()
                .map(path -> path.getName() + "=" + path)
                .collect(Collectors.joining(","));

        // Setup CLASSPATH and environment variables for ApplicationMaster
        final Map<String, String> appMasterEnv = setUpAmEnvironment(
                appId,
                classPath,shippedFiles,
                //"","",
                getDynamicPropertiesEncoded()
        );

        amContainer.setEnvironment(appMasterEnv);

        // Set up resource type requirements for ApplicationMaster
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(appConf.getJobManagerMemoryMb());  //设置jobManneger
        capability.setVirtualCores(1);  //默认是1

        appContext.setApplicationName(jobName);
        appContext.setApplicationType(APPLICATION_TYPE);
        appContext.setAMContainerSpec(amContainer);
        appContext.setResource(capability);
        //appContext.setApplicationTags(appConf.getAppTags());
        if (appConf.getQueue() != null) {
            appContext.setQueue(appConf.getQueue());
        }

        LOG.info("Submitting application master {}", appId);
        yarnClient.submitApplication(appContext);

        PollDeploymentStatus poll = new PollDeploymentStatus(appId);
        YARN_POLL_EXECUTOR.submit(poll);
        try {
            return poll.result.get();
        }
        catch (ExecutionException e) {
            LOG.warn("Failed to deploy {}, cause: {}", appId.toString(), e.getCause());
            yarnClient.killApplication(appId);
            throw (Exception) e.getCause();
        }
    }

    private void collectLocalResources(
            Map<String, LocalResource> resources,
            Set<Path> shippedPaths
    )
            throws IOException, URISyntaxException
    {
        if(clusterConf.flinkJar() != null) {
            Path flinkJar = clusterConf.flinkJar();
            LocalResource flinkJarResource = setupLocalResource(flinkJar, homedir, ""); //放到 Appid/根目录下
            this.flinkJar = ConverterUtils.getPathFromYarnURL(flinkJarResource.getResource());
            resources.put("flink.jar", flinkJarResource);
        }
        if(clusterConf.resourcesToLocalize() != null) {
            for (Path p : clusterConf.resourcesToLocalize()) {  //主要是 flink.jar log4f.propors 和 flink.yaml 三个文件
                LocalResource resource = setupLocalResource(p, homedir, ""); //这些需要放到根目录下
                resources.put(p.getName(), resource);
                if ("log4j.properties".equals(p.getName())) {
                    shippedPaths.add(ConverterUtils.getPathFromYarnURL(resource.getResource()));
                }
            }
        }
        if(userProvidedJars != null) {
            for (Path p : userProvidedJars) {
                String name = p.getName();
                if (resources.containsKey(name)) {   //这里当jar 有重复的时候 会抛出异常
                    LOG.warn("Duplicated name in the shipped files {}", p);
                } else {
                    LocalResource resource = setupLocalResource(p, homedir, "jars"); //这些放到 jars目录下
                    resources.put(name, resource);
                    shippedPaths.add(ConverterUtils.getPathFromYarnURL(resource.getResource()));
                }
            }
        }
    }

    private LocalResource registerLocalResource(FileSystem fs, Path remoteRsrcPath)
            throws IOException
    {
        LocalResource localResource = Records.newRecord(LocalResource.class);
        FileStatus jarStat = fs.getFileStatus(remoteRsrcPath);
        localResource.setResource(ConverterUtils.getYarnUrlFromURI(remoteRsrcPath.toUri()));
        localResource.setSize(jarStat.getLen());
        localResource.setTimestamp(jarStat.getModificationTime());
        localResource.setType(LocalResourceType.FILE);
        localResource.setVisibility(LocalResourceVisibility.APPLICATION);
        return localResource;
    }

    private LocalResource setupLocalResource(
            Path localSrcPath,
            Path homedir,
            String relativeTargetPath)
            throws IOException
    {
        if (new File(localSrcPath.toUri().getPath()).isDirectory()) {
            throw new IllegalArgumentException("File to copy must not be a directory: " +
                    localSrcPath);
        }

        // copy resource to HDFS
        String suffix = "." + (relativeTargetPath.isEmpty() ? "" : "/" + relativeTargetPath)
                + "/" + localSrcPath.getName();

        Path dst = new Path(homedir, suffix);

        LOG.info("Uploading {}", dst);

        FileSystem hdfs = FileSystem.get(clusterConf.yarnConf());
        hdfs.copyFromLocalFile(false, true, localSrcPath, dst);

        // now create the resource instance
        LocalResource resource = registerLocalResource(hdfs, dst);
        return resource;
    }

    private Map<String, String> setUpAmEnvironment(
            ApplicationId appId,
            String amClassPath,
            String shipFiles,
            String dynamicProperties)
            throws IOException, URISyntaxException
    {
        final Map<String, String> appMasterEnv = new HashMap<>();

        // set Flink app class path
        appMasterEnv.put(YarnConfigKeys.ENV_FLINK_CLASSPATH, amClassPath);

        // set Flink on YARN internal configuration values
        appMasterEnv.put(YarnConfigKeys.ENV_TM_COUNT, String.valueOf(appConf.getTaskManagerCount()));
        appMasterEnv.put(YarnConfigKeys.ENV_TM_MEMORY, String.valueOf(appConf.getTaskManagerMemoryMb()));
        appMasterEnv.put(YarnConfigKeys.ENV_SLOTS, String.valueOf(appConf.getTaskManagerSlots()));
        appMasterEnv.put(YarnConfigKeys.FLINK_JAR_PATH, flinkJar.toString());
        appMasterEnv.put(YarnConfigKeys.ENV_APP_ID, appId.toString());
        appMasterEnv.put(YarnConfigKeys.ENV_CLIENT_HOME_DIR, homedir.toString()); //$home/.flink/appid 这个目录里面存放临时数据
        appMasterEnv.put(YarnConfigKeys.ENV_CLIENT_SHIP_FILES, shipFiles);

        appMasterEnv.put(YarnConfigKeys.ENV_DETACHED, String.valueOf(true));  //是否分离 分离就cluser模式 否则是client模式

        appMasterEnv.put(YarnConfigKeys.ENV_HADOOP_USER_NAME,
                UserGroupInformation.getCurrentUser().getUserName());

        if (dynamicProperties != null) {
            appMasterEnv.put(YarnConfigKeys.ENV_DYNAMIC_PROPERTIES, dynamicProperties);
        }

        // set classpath from YARN configuration
        Utils.setupYarnClassPath(clusterConf.yarnConf(), appMasterEnv);

        return appMasterEnv;
    }

    /**
     * flink 1.5 add
     */
    @Override
    public ClusterClient<ApplicationId> deployJobCluster(ClusterSpecification clusterSpecification, JobGraph jobGraph, boolean detached)
            throws ClusterDeploymentException
    {
        throw new UnsupportedOperationException("this method have't support!");
    }

    private final class PollDeploymentStatus
            implements Runnable
    {
        private final CompletableFuture<ApplicationReport> result = new CompletableFuture<>();
        private final ApplicationId appId;
        private YarnApplicationState lastAppState = NEW;
        private long startTime;

        private PollDeploymentStatus(ApplicationId appId)
        {
            this.appId = appId;
        }

        @Override
        public void run()
        {
            if (startTime == 0) {
                startTime = System.currentTimeMillis();
            }

            try {
                ApplicationReport report = poll();
                if (report == null) {
                    YARN_POLL_EXECUTOR.schedule(this, RETRY_DELAY_MS, TimeUnit.MILLISECONDS);
                }
                else {
                    result.complete(report);
                }
            }
            catch (YarnException | IOException e) {
                result.completeExceptionally(e);
            }
        }

        private ApplicationReport poll()
                throws IOException, YarnException
        {
            ApplicationReport report;
            report = yarnClient.getApplicationReport(appId);
            YarnApplicationState appState = report.getYarnApplicationState();
            LOG.debug("Application State: {}", appState);

            switch (appState) {
                case FAILED:
                case FINISHED:
                    //TODO: the finished state may be valid in flip-6
                case KILLED:
                    throw new IOException("The YARN application unexpectedly switched to state "
                            + appState + " during deployment. \n"
                            + "Diagnostics from YARN: " + report.getDiagnostics() + "\n"
                            + "If log aggregation is enabled on your cluster, use this command to further investigate the issue:\n"
                            + "yarn logs -applicationId " + appId);
                    //break ..
                case RUNNING:
                    LOG.info("YARN application has been deployed successfully.");
                    break;
                default:
                    if (appState != lastAppState) {
                        LOG.info("Deploying cluster, current state " + appState);
                    }
                    lastAppState = appState;
                    if (System.currentTimeMillis() - startTime > DEPLOY_TIMEOUT_MS) {
                        throw new RuntimeException(String.format("Deployment took more than %d seconds. "
                                + "Please check if the requested resources are available in the YARN cluster", DEPLOY_TIMEOUT_MS));
                    }
                    return null;
            }
            return report;
        }
    }
}
