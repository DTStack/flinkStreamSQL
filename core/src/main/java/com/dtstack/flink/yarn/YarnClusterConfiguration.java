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

import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.util.Set;

public class YarnClusterConfiguration {
    /**
     * The configuration used by YARN (i.e., <pre>yarn-site.xml</pre>).
     */
    private final YarnConfiguration yarnConf;

    /**
     * The home directory of all job where all the temporary files for each jobs are stored.
     */
    private final String appRootDir;

    /**
     * Thepublic YarnClusterConfiguration(Configuration , YarnConfiguration , String , Path , Set<Path> ) {
    } location of the Flink jar.
     */
    private final Path flinkJar;

    /**
     * Additional resources to be localized for both JobManager and TaskManager.
     * They will NOT be added into the classpaths.
     */
    private final Set<Path> resourcesToLocalize;

    /**
     * flink conf
     */
    private final Configuration flinkConfiguration;

    public YarnClusterConfiguration(
            Configuration flinkConf,
            YarnConfiguration conf,
            String appRootDir,
            Path flinkJar,
            Set<Path> resourcesToLocalize) {
        this.flinkConfiguration = flinkConf;
        this.yarnConf = conf;
        this.appRootDir = appRootDir;
        this.flinkJar = flinkJar;
        this.resourcesToLocalize = resourcesToLocalize;
    }



    YarnConfiguration yarnConf() {
        return yarnConf;
    }

    public String appRootDir() {
        return appRootDir;
    }

    public Configuration flinkConfiguration() {
        return flinkConfiguration;
    }

    public Path flinkJar() {
        return flinkJar;
    }

    public Set<Path> resourcesToLocalize() {
        return resourcesToLocalize;
    }

}
