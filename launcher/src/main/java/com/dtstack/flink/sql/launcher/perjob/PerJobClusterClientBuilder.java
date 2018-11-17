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

import com.dtstack.flink.sql.launcher.YarnConfLoader;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.hadoop.shaded.com.google.common.base.Strings;
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
import java.util.Properties;

/**
 * Reason:
 * Date: 2018/11/16
 * Company: www.dtstack.com
 * @author xuchao
 */

public class PerJobClusterClientBuilder {

    public static final String DEFAULT_GATEWAY_CLASS = "org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporter";

    public static final String PROMGATEWAY_CLASS_KEY = "metrics.reporter.promgateway.class";

    public static final String PROMGATEWAY_HOST_KEY = "metrics.reporter.promgateway.host";

    public static final String PROMGATEWAY_PORT_KEY = "metrics.reporter.promgateway.port";

    public static final String PROMGATEWAY_JOBNAME_KEY = "metrics.reporter.promgateway.jobName";

    public static final String PROMGATEWAY_RANDOMJOBNAMESUFFIX_KEY = "metrics.reporter.promgateway.randomJobNameSuffix";

    public static final String PROMGATEWAY_DELETEONSHUTDOWN_KEY = "metrics.reporter.promgateway.deleteOnShutdown";

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

    public AbstractYarnClusterDescriptor createPerJobClusterDescriptor(Properties confProp, String flinkJarPath, String queue) throws MalformedURLException {
        Configuration newConf = new Configuration();
        newConf.addAllToProperties(confProp);

        //perJobMetricConfigConfig(newConf, properties);
        AbstractYarnClusterDescriptor clusterDescriptor = getClusterDescriptor(newConf, yarnConf, ".");

        if (StringUtils.isNotBlank(flinkJarPath)) {

            if (!new File(flinkJarPath).exists()) {
                throw new RuntimeException("The Flink jar path is not exist");
            }

        }

        List<URL> classpaths = new ArrayList<>();
        if (flinkJarPath != null) {
            File[] jars = new File(flinkJarPath).listFiles();

            for (File file : jars){
                if (file.toURI().toURL().toString().contains("flink-dist")){
                    clusterDescriptor.setLocalJarPath(new Path(file.toURI().toURL().toString()));
                } else {
                    classpaths.add(file.toURI().toURL());
                }
            }

        } else {
            throw new RuntimeException("The Flink jar path is null");
        }

        clusterDescriptor.setProvidedUserJarFiles(classpaths);

        if(!Strings.isNullOrEmpty(queue)){
            clusterDescriptor.setQueue(queue);
        }
        return clusterDescriptor;
    }

    //FIXME need?
    private void perJobMetricConfigConfig(Configuration configuration, Properties properties){
        if(!properties.containsKey(DEFAULT_GATEWAY_CLASS)){
            return;
        }

        configuration.setString(PROMGATEWAY_CLASS_KEY, properties.getProperty(PROMGATEWAY_CLASS_KEY));
        configuration.setString(PROMGATEWAY_HOST_KEY,  properties.getProperty(PROMGATEWAY_HOST_KEY));
        configuration.setString(PROMGATEWAY_PORT_KEY, properties.getProperty(PROMGATEWAY_PORT_KEY));
        configuration.setString(PROMGATEWAY_JOBNAME_KEY, properties.getProperty(PROMGATEWAY_JOBNAME_KEY));
        configuration.setString(PROMGATEWAY_RANDOMJOBNAMESUFFIX_KEY, properties.getProperty(PROMGATEWAY_RANDOMJOBNAMESUFFIX_KEY));
        configuration.setString(PROMGATEWAY_DELETEONSHUTDOWN_KEY, properties.getProperty(PROMGATEWAY_DELETEONSHUTDOWN_KEY));
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
