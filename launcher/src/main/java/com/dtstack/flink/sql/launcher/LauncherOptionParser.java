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

import avro.shaded.com.google.common.collect.Lists;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.hadoop.shaded.com.google.common.base.Charsets;
import org.apache.flink.hadoop.shaded.com.google.common.base.Preconditions;

import java.io.File;
import java.io.FileInputStream;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.dtstack.flink.sql.launcher.LauncherOptions.*;
import static com.dtstack.flink.sql.launcher.ClusterMode.*;


/**
 * The Parser of Launcher commandline options
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class LauncherOptionParser {

    private Options options = new Options();

    private BasicParser parser = new BasicParser();

    private Properties properties = new Properties();

    public LauncherOptionParser(String[] args) {
        options.addOption(LauncherOptions.OPTION_MODE, true, "Running mode");
        options.addOption(OPTION_SQL, true, "Job sql file");
        options.addOption(OPTION_NAME, true, "Job name");
        options.addOption(OPTION_FLINK_CONF_DIR, true, "Flink configuration directory");
        options.addOption(OPTION_LOCAL_SQL_PLUGIN_PATH, true, "sql local plugin root");
        options.addOption(OPTION_REMOTE_SQL_PLUGIN_PATH, true, "sql remote plugin root");
        options.addOption(OPTION_ADDJAR, true, "sql ext jar,eg udf jar");
        options.addOption(OPTION_CONF_PROP, true, "sql ref prop,eg specify event time");
        options.addOption(OPTION_YARN_CONF_DIR, true, "Yarn and hadoop configuration directory");

        try {
            CommandLine cl = parser.parse(options, args);
            String mode = cl.getOptionValue(OPTION_MODE, MODE_LOCAL);
            //check mode
            properties.put(OPTION_MODE, mode);

            String job = Preconditions.checkNotNull(cl.getOptionValue(OPTION_SQL),
                    "Must specify job file using option '" + OPTION_SQL + "'");
            File file = new File(job);
            FileInputStream in = new FileInputStream(file);
            byte[] filecontent = new byte[(int) file.length()];
            in.read(filecontent);
            String content = new String(filecontent, "UTF-8");
            String sql = URLEncoder.encode(content, Charsets.UTF_8.name());
            properties.put(OPTION_SQL, sql);

            String localPlugin = Preconditions.checkNotNull(cl.getOptionValue(OPTION_LOCAL_SQL_PLUGIN_PATH));
            properties.put(OPTION_LOCAL_SQL_PLUGIN_PATH, localPlugin);

            String remotePlugin = Preconditions.checkNotNull(cl.getOptionValue(OPTION_REMOTE_SQL_PLUGIN_PATH));
            properties.put(OPTION_REMOTE_SQL_PLUGIN_PATH, remotePlugin);

            String name = Preconditions.checkNotNull(cl.getOptionValue(OPTION_NAME));
            properties.put(OPTION_NAME, name);

            String addJar = cl.getOptionValue(OPTION_ADDJAR);
            if(StringUtils.isNotBlank(addJar)){
                properties.put(OPTION_ADDJAR, addJar);
            }

            String confProp = cl.getOptionValue(OPTION_CONF_PROP);
            if(StringUtils.isNotBlank(confProp)){
                properties.put(OPTION_CONF_PROP, confProp);
            }

            String flinkConfDir = cl.getOptionValue(OPTION_FLINK_CONF_DIR);
            if(StringUtils.isNotBlank(flinkConfDir)) {
                properties.put(OPTION_FLINK_CONF_DIR, flinkConfDir);
            }

            String yarnConfDir = cl.getOptionValue(OPTION_YARN_CONF_DIR);
            if(StringUtils.isNotBlank(yarnConfDir)) {
                properties.put(OPTION_YARN_CONF_DIR, yarnConfDir);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public Properties getProperties(){
        return properties;
    }

    public Object getVal(String key){
        return properties.get(key);
    }

    public List<String> getAllArgList(){
        List<String> args = Lists.newArrayList();
        for(Map.Entry<Object, Object> one : properties.entrySet()){
            args.add("-" + one.getKey().toString());
            args.add(one.getValue().toString());
        }

        return args;
    }

    public List<String> getProgramExeArgList(){
        List<String> args = Lists.newArrayList();
        for(Map.Entry<Object, Object> one : properties.entrySet()){
            String key = one.getKey().toString();
            if(OPTION_FLINK_CONF_DIR.equalsIgnoreCase(key)
                    || OPTION_YARN_CONF_DIR.equalsIgnoreCase(key)){
                continue;
            }

            args.add("-" + key);
            args.add(one.getValue().toString());
        }

        return args;
    }

}
