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

package com.dtstack.flink.sql.launcher.entity;

import java.util.Arrays;
import java.util.Properties;

/**
 * parse the original mission parameters
 * Date: 2020/3/4
 * Company: www.dtstack.com
 *
 * @author maqi
 */
public class JobParamsInfo {

    private final String mode;
    private final String name;
    private final String queue;
    private final String localPluginRoot;
    private final String flinkConfDir;
    private final String flinkJarPath;
    private final String yarnConfDir;
    private final String pluginLoadMode;
    private final String udfJar;
    private final String[] execArgs;
    private final Properties confProperties;
    private final Properties yarnSessionConfProperties;
    private final String addShipFile;
    private final String checkResource;

    private JobParamsInfo(String mode, String name, String queue, String localPluginRoot, String flinkConfDir, String yarnConfDir,
                          String pluginLoadMode, String[] execArgs, Properties confProperties, Properties yarnSessionConfProperties,
                          String udfJar, String flinkJarPath, String addShipFile, String checkResource) {
        this.mode = mode;
        this.name = name;
        this.queue = queue;
        this.localPluginRoot = localPluginRoot;
        this.flinkConfDir = flinkConfDir;
        this.yarnConfDir = yarnConfDir;
        this.pluginLoadMode = pluginLoadMode;
        this.execArgs = execArgs;
        this.confProperties = confProperties;
        this.yarnSessionConfProperties = yarnSessionConfProperties;
        this.udfJar = udfJar;
        this.flinkJarPath = flinkJarPath;
        this.addShipFile = addShipFile;
        this.checkResource = checkResource;
    }

    public String getMode() {
        return mode;
    }

    public String getName() {
        return name;
    }

    public String getQueue() {
        return queue;
    }

    public String getLocalPluginRoot() {
        return localPluginRoot;
    }

    public String getFlinkConfDir() {
        return flinkConfDir;
    }

    public String getYarnConfDir() {
        return yarnConfDir;
    }

    public String getPluginLoadMode() {
        return pluginLoadMode;
    }

    public String[] getExecArgs() {
        return execArgs;
    }

    public Properties getConfProperties() {
        return confProperties;
    }

    public Properties getYarnSessionConfProperties() {
        return yarnSessionConfProperties;
    }

    public String getUdfJar() {
        return udfJar;
    }

    public String getFlinkJarPath() {
        return flinkJarPath;
    }

    public String getAddShipFile() {
        return addShipFile;
    }

    public String getCheckResource() {
        return checkResource;
    }

    public static JobParamsInfo.Builder builder() {
        return new JobParamsInfo.Builder();
    }


    public static class Builder {
        private String mode;
        private String name;
        private String queue;
        private String localPluginRoot;
        private String flinkConfDir;
        private String flinkJarPath;
        private String yarnConfDir;
        private String pluginLoadMode;
        private String[] execArgs;
        private String udfJar;
        private Properties confProperties;
        private Properties yarnSessionConfProperties;
        private String addShipfile;
        private String checkResource;

        public JobParamsInfo.Builder setMode(String mode) {
            this.mode = mode;
            return this;
        }

        public JobParamsInfo.Builder setName(String name) {
            this.name = name;
            return this;
        }

        public JobParamsInfo.Builder setQueue(String queue) {
            this.queue = queue;
            return this;
        }

        public JobParamsInfo.Builder setLocalPluginRoot(String localPluginRoot) {
            this.localPluginRoot = localPluginRoot;
            return this;
        }

        public JobParamsInfo.Builder setFlinkConfDir(String flinkConfDir) {
            this.flinkConfDir = flinkConfDir;
            return this;
        }

        public JobParamsInfo.Builder setYarnConfDir(String yarnConfDir) {
            this.yarnConfDir = yarnConfDir;
            return this;
        }

        public JobParamsInfo.Builder setPluginLoadMode(String pluginLoadMode) {
            this.pluginLoadMode = pluginLoadMode;
            return this;
        }

        public JobParamsInfo.Builder setExecArgs(String[] execArgs) {
            this.execArgs = execArgs;
            return this;
        }

        public JobParamsInfo.Builder setConfProperties(Properties confProperties) {
            this.confProperties = confProperties;
            return this;
        }

        public JobParamsInfo.Builder setYarnSessionConfProperties(Properties yarnSessionConfProperties) {
            this.yarnSessionConfProperties = yarnSessionConfProperties;
            return this;
        }

        public JobParamsInfo.Builder setUdfJar(String udfJar) {
            this.udfJar = udfJar;
            return this;
        }

        public JobParamsInfo.Builder setFlinkJarPath(String flinkJarPath) {
            this.flinkJarPath = flinkJarPath;
            return this;
        }

        public JobParamsInfo.Builder setAddShipfile(String addShipfile) {
            this.addShipfile = addShipfile;
            return this;
        }

        public JobParamsInfo.Builder setCheckResource(String checkResource) {
            this.checkResource = checkResource;
            return this;
        }

        public JobParamsInfo build() {
            return new JobParamsInfo(mode, name, queue, localPluginRoot, flinkConfDir,
                    yarnConfDir, pluginLoadMode, execArgs, confProperties,
                    yarnSessionConfProperties, udfJar, flinkJarPath, addShipfile,
                    checkResource);
        }
    }

    @Override
    public String toString() {
        return "JobParamsInfo{" +
                "mode='" + mode + '\'' +
                ", name='" + name + '\'' +
                ", queue='" + queue + '\'' +
                ", localPluginRoot='" + localPluginRoot + '\'' +
                ", flinkConfDir='" + flinkConfDir + '\'' +
                ", flinkJarPath='" + flinkJarPath + '\'' +
                ", yarnConfDir='" + yarnConfDir + '\'' +
                ", pluginLoadMode='" + pluginLoadMode + '\'' +
                ", udfJar='" + udfJar + '\'' +
                ", execArgs=" + Arrays.toString(execArgs) +
                ", confProperties=" + confProperties +
                ", yarnSessionConfProperties=" + yarnSessionConfProperties +
                ", addShipFile='" + addShipFile + '\'' +
                ", checkResource='" + checkResource + '\'' +
                '}';
    }
}
