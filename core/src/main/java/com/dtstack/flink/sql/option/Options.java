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

package com.dtstack.flink.sql.option;

import com.dtstack.flink.sql.enums.ClusterMode;
import com.dtstack.flink.sql.enums.EPluginLoadMode;


/**
 * This class define commandline options for the Launcher program
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class Options {

    @OptionRequired(description = "Running mode")
    private String mode = ClusterMode.local.name();

    @OptionRequired(required = true, description = "Job name")
    private String name;

    @OptionRequired(required = true, description = "Job sql file")
    private String sql;

    @OptionRequired(description = "Flink configuration directory")
    private String flinkconf;

    @OptionRequired(description = "Yarn and Hadoop configuration directory")
    private String yarnconf;

    @OptionRequired(description = "Sql local plugin root")
    private String localSqlPluginPath;

    @OptionRequired(description = "Sql remote plugin root")
    private String remoteSqlPluginPath;

    @OptionRequired(description = "sql ext jar,eg udf jar")
    private String addjar;

    @OptionRequired(description = "sql ref prop,eg specify event time")
    private String confProp = "{}";

    @OptionRequired(description = "flink jar path for submit of perjob mode")
    private String flinkJarPath;

    @OptionRequired(description = "yarn queue")
    private String queue = "default";

    @OptionRequired(description = "yarn session configuration,such as yid")
    private String yarnSessionConf = "{}";

    @OptionRequired(description = "plugin load mode, by classpath or shipfile")
    private String pluginLoadMode = EPluginLoadMode.CLASSPATH.name();

    @OptionRequired(description = "file add to ship file")
    private String addShipfile;

    @OptionRequired(description = "dirty plugin properties")
    private String dirtyProperties;

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getFlinkconf() {
        return flinkconf;
    }

    public void setFlinkconf(String flinkconf) {
        this.flinkconf = flinkconf;
    }

    public String getYarnconf() {
        return yarnconf;
    }

    public void setYarnconf(String yarnconf) {
        this.yarnconf = yarnconf;
    }

    public String getLocalSqlPluginPath() {
        return localSqlPluginPath;
    }

    public void setLocalSqlPluginPath(String localSqlPluginPath) {
        this.localSqlPluginPath = localSqlPluginPath;
    }

    public String getRemoteSqlPluginPath() {
        return remoteSqlPluginPath;
    }

    public void setRemoteSqlPluginPath(String remoteSqlPluginPath) {
        this.remoteSqlPluginPath = remoteSqlPluginPath;
    }

    public String getAddjar() {
        return addjar;
    }

    public void setAddjar(String addjar) {
        this.addjar = addjar;
    }

    public String getConfProp() {
        return confProp;
    }

    public void setConfProp(String confProp) {
        this.confProp = confProp;
    }

    public String getFlinkJarPath() {
        return flinkJarPath;
    }

    public void setFlinkJarPath(String flinkJarPath) {
        this.flinkJarPath = flinkJarPath;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public String getYarnSessionConf() {
        return yarnSessionConf;
    }

    public void setYarnSessionConf(String yarnSessionConf) {
        this.yarnSessionConf = yarnSessionConf;
    }

    public String getPluginLoadMode() {
        return pluginLoadMode;
    }

    public void setPluginLoadMode(String pluginLoadMode) {
        this.pluginLoadMode = pluginLoadMode;
    }

    public String getAddShipfile() {
        return addShipfile;
    }

    public void setAddShipfile(String addShipfile) {
        this.addShipfile = addShipfile;
    }

    public String getDirtyProperties() {
        return dirtyProperties;
    }

    public void setDirtyProperties(String dirtyProperties) {
        this.dirtyProperties = dirtyProperties;
    }
}
