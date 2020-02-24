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

package com.dtstack.flink.sql.exec;


import java.net.URL;
import java.util.List;
import java.util.Properties;

/**
 * 解析传递的参数信息
 * Date: 2020/2/24
 * Company: www.dtstack.com
 * @author maqi
 */
public class ParamsInfo {

    private String sql;
    private String name;
    private List<URL> jarUrlList;
    private String localSqlPluginPath;
    private String remoteSqlPluginPath;
    private String pluginLoadMode;
    private String deployMode;
    private Properties confProp;

    public ParamsInfo(String sql, String name, List<URL> jarUrlList, String localSqlPluginPath,
                      String remoteSqlPluginPath, String pluginLoadMode, String deployMode, Properties confProp) {
        this.sql = sql;
        this.name = name;
        this.jarUrlList = jarUrlList;
        this.localSqlPluginPath = localSqlPluginPath;
        this.remoteSqlPluginPath = remoteSqlPluginPath;
        this.pluginLoadMode = pluginLoadMode;
        this.deployMode = deployMode;
        this.confProp = confProp;
    }

    public String getSql() {
        return sql;
    }

    public String getName() {
        return name;
    }

    public List<URL> getJarUrlList() {
        return jarUrlList;
    }

    public String getLocalSqlPluginPath() {
        return localSqlPluginPath;
    }

    public String getRemoteSqlPluginPath() {
        return remoteSqlPluginPath;
    }

    public String getPluginLoadMode() {
        return pluginLoadMode;
    }

    public String getDeployMode() {
        return deployMode;
    }

    public Properties getConfProp() {
        return confProp;
    }

    @Override
    public String toString() {
        return "ParamsInfo{" +
                "sql='" + sql + '\'' +
                ", name='" + name + '\'' +
                ", jarUrlList=" + convertJarUrlListToString(jarUrlList) +
                ", localSqlPluginPath='" + localSqlPluginPath + '\'' +
                ", remoteSqlPluginPath='" + remoteSqlPluginPath + '\'' +
                ", pluginLoadMode='" + pluginLoadMode + '\'' +
                ", deployMode='" + deployMode + '\'' +
                ", confProp=" + confProp +
                '}';
    }

    public String convertJarUrlListToString(List<URL> jarUrlList) {
        return jarUrlList.stream().map(URL::toString).reduce((pre, last) -> pre + last).orElse("");
    }

    public static ParamsInfo.Builder builder() {
        return new ParamsInfo.Builder();
    }
    public static class Builder {

        private String sql;
        private String name;
        private List<URL> jarUrlList;
        private String localSqlPluginPath;
        private String remoteSqlPluginPath;
        private String pluginLoadMode;
        private String deployMode;
        private Properties confProp;


        public ParamsInfo.Builder setSql(String sql) {
            this.sql = sql;
            return this;
        }

        public ParamsInfo.Builder setName(String name) {
            this.name = name;
            return this;
        }

        public ParamsInfo.Builder setJarUrlList(List<URL> jarUrlList) {
            this.jarUrlList = jarUrlList;
            return this;
        }

        public ParamsInfo.Builder setLocalSqlPluginPath(String localSqlPluginPath) {
            this.localSqlPluginPath = localSqlPluginPath;
            return this;
        }

        public ParamsInfo.Builder setRemoteSqlPluginPath(String remoteSqlPluginPath) {
            this.remoteSqlPluginPath = remoteSqlPluginPath;
            return this;
        }

        public ParamsInfo.Builder setPluginLoadMode(String pluginLoadMode) {
            this.pluginLoadMode = pluginLoadMode;
            return this;
        }

        public ParamsInfo.Builder setDeployMode(String deployMode) {
            this.deployMode = deployMode;
            return this;
        }

        public ParamsInfo.Builder setConfProp(Properties confProp) {
            this.confProp = confProp;
            return this;
        }

        public ParamsInfo build() {
            return new ParamsInfo(sql, name, jarUrlList, localSqlPluginPath,
                    remoteSqlPluginPath, pluginLoadMode, deployMode, confProp);
        }
    }
}
