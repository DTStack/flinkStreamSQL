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

    public static final String OPTION_MODE = "mode";

    public static final String OPTION_NAME = "name";

    public static final String OPTION_SQL = "sql";

    public static final String OPTION_FLINK_CONF_DIR = "flinkconf";

    public static final String OPTION_YARN_CONF_DIR = "yarnconf";

    public static final String OPTION_LOCAL_SQL_PLUGIN_PATH = "localSqlPluginPath";

    public static final String OPTION_REMOTE_SQL_PLUGIN_PATH = "remoteSqlPluginPath";

    public static final String OPTION_ADDJAR = "addjar";

    public static final String OPTION_CONF_PROP = "confProp";


}
