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

package com.dtstack.flink.sql.launcher;

/**
 * This class define commandline options for the Launcher program
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class LauncherOptions {

    private  String mode;

    private  String name;

    private  String sql;

    private  String flinkconf;

    private  String yarnconf;

    private  String localSqlPluginPath;

    private  String remoteSqlPluginPath ;

    private  String addjar;

    private  String confProp;

    private  String savePointPath;

    private  String allowNonRestoredState = "false";

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

    public String getSavePointPath() {
        return savePointPath;
    }

    public void setSavePointPath(String savePointPath) {
        this.savePointPath = savePointPath;
    }

    public String getAllowNonRestoredState() {
        return allowNonRestoredState;
    }

    public void setAllowNonRestoredState(String allowNonRestoredState) {
        this.allowNonRestoredState = allowNonRestoredState;
    }
}
