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
import com.dtstack.flink.sql.util.PluginUtil;
import java.io.File;
import java.io.FileInputStream;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;
import com.dtstack.flink.sql.ClusterMode;



/**
 * The Parser of Launcher commandline options
 *
 * Company: www.dtstack.com
 * @author sishu.yss
 */
public class LauncherOptionParser {

    public static final String OPTION_MODE = "mode";

    public static final String OPTION_NAME = "name";

    public static final String OPTION_SQL = "sql";

    public static final String OPTION_FLINK_CONF_DIR = "flinkconf";

    public static final String OPTION_YARN_CONF_DIR = "yarnconf";

    public static final String OPTION_LOCAL_SQL_PLUGIN_PATH = "localSqlPluginPath";

    public static final String OPTION_REMOTE_SQL_PLUGIN_PATH = "remoteSqlPluginPath";

    public static final String OPTION_ADDJAR = "addjar";

    public static final String OPTION_CONF_PROP = "confProp";

    public static final String OPTION_SAVE_POINT_PATH = "savePointPath";

    public static final String OPTION_ALLOW_NON_RESTORED_STATE = "allowNonRestoredState";

    public static final String HADOOP_USER_NAME = "hadoop_user_name";

    private Options options = new Options();

    private BasicParser parser = new BasicParser();

    private LauncherOptions properties = new LauncherOptions();

    public LauncherOptionParser(String[] args) {
        options.addOption(OPTION_MODE, true, "Running mode");
        options.addOption(OPTION_SQL, true, "Job sql file");
        options.addOption(OPTION_NAME, true, "Job name");
        options.addOption(OPTION_FLINK_CONF_DIR, true, "Flink configuration directory");
        options.addOption(OPTION_LOCAL_SQL_PLUGIN_PATH, true, "sql local plugin root");
        options.addOption(OPTION_REMOTE_SQL_PLUGIN_PATH, true, "sql remote plugin root");
        options.addOption(OPTION_ADDJAR, true, "sql ext jar,eg udf jar");
        options.addOption(OPTION_CONF_PROP, true, "sql ref prop,eg specify event time");
        options.addOption(OPTION_YARN_CONF_DIR, true, "Yarn and hadoop configuration directory");
        options.addOption(HADOOP_USER_NAME, true, "hadoop user");
        options.addOption(OPTION_SAVE_POINT_PATH, true, "Savepoint restore path");
        options.addOption(OPTION_ALLOW_NON_RESTORED_STATE, true, "Flag indicating whether non restored state is allowed if the savepoint");

        try {
            CommandLine cl = parser.parse(options, args);
            String mode = cl.getOptionValue(OPTION_MODE, ClusterMode.local.name());
            //check mode
            properties.setMode(mode);

            String job = Preconditions.checkNotNull(cl.getOptionValue(OPTION_SQL),
                    "Must specify job file using option '" + OPTION_SQL + "'");
            File file = new File(job);
            FileInputStream in = new FileInputStream(file);
            byte[] filecontent = new byte[(int) file.length()];
            in.read(filecontent);
            String content = new String(filecontent, "UTF-8");
            String sql = URLEncoder.encode(content, Charsets.UTF_8.name());
            properties.setSql(sql);
            String localPlugin = Preconditions.checkNotNull(cl.getOptionValue(OPTION_LOCAL_SQL_PLUGIN_PATH));
            properties.setLocalSqlPluginPath(localPlugin);
            String remotePlugin = cl.getOptionValue(OPTION_REMOTE_SQL_PLUGIN_PATH);
            properties.setRemoteSqlPluginPath(remotePlugin);
            String name = Preconditions.checkNotNull(cl.getOptionValue(OPTION_NAME));
            properties.setName(name);
            String addJar = cl.getOptionValue(OPTION_ADDJAR);
            if(StringUtils.isNotBlank(addJar)){
                properties.setAddjar(addJar);
            }
            String confProp = cl.getOptionValue(OPTION_CONF_PROP);
            if(StringUtils.isNotBlank(confProp)){
                properties.setConfProp(confProp);
            }
            String flinkConfDir = cl.getOptionValue(OPTION_FLINK_CONF_DIR);
            if(StringUtils.isNotBlank(flinkConfDir)) {
                properties.setFlinkconf(flinkConfDir);
            }

            String yarnConfDir = cl.getOptionValue(OPTION_YARN_CONF_DIR);
            if(StringUtils.isNotBlank(yarnConfDir)) {
                properties.setYarnconf(yarnConfDir);
            }

            String savePointPath = cl.getOptionValue(OPTION_SAVE_POINT_PATH);
            if(StringUtils.isNotBlank(savePointPath)) {
                properties.setSavePointPath(savePointPath);
            }

            String allow_non = cl.getOptionValue(OPTION_ALLOW_NON_RESTORED_STATE);
            if(StringUtils.isNotBlank(allow_non)) {
                properties.setAllowNonRestoredState(allow_non);
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public LauncherOptions getLauncherOptions(){
        return properties;
    }

    public List<String> getProgramExeArgList() throws Exception {
        Map<String,Object> mapConf = PluginUtil.ObjectToMap(properties);
        List<String> args = Lists.newArrayList();
        for(Map.Entry<String, Object> one : mapConf.entrySet()){
            String key = one.getKey();
            if(OPTION_FLINK_CONF_DIR.equalsIgnoreCase(key)){
                continue;
            }

            if(one.getValue() == null){
                continue;
            }

            args.add("-" + key);
            args.add(one.getValue().toString());
        }
        return args;
    }
}
