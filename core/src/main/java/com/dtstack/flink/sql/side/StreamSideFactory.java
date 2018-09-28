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

 

package com.dtstack.flink.sql.side;

import com.dtstack.flink.sql.classloader.DtClassLoader;
import com.dtstack.flink.sql.table.AbsSideTableParser;
import com.dtstack.flink.sql.table.AbsTableParser;
import com.dtstack.flink.sql.util.PluginUtil;

/**
 * get specify side parser
 * Date: 2018/7/25
 * Company: www.dtstack.com
 * @author xuchao
 */

public class StreamSideFactory {

    private static final String CURR_TYPE = "side";

    private static final String SIDE_DIR_TMPL = "%s%sside";

    public static AbsTableParser getSqlParser(String pluginType, String sqlRootDir, String cacheType) throws Exception {

        cacheType = cacheType == null ? "async" : cacheType;
        String sideDir = String.format(SIDE_DIR_TMPL, pluginType, cacheType);
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        String pluginJarPath = PluginUtil.getJarFileDirPath(sideDir, sqlRootDir);

        DtClassLoader dtClassLoader = (DtClassLoader) classLoader;
        PluginUtil.addPluginJar(pluginJarPath, dtClassLoader);
        String className = PluginUtil.getSqlParserClassName(pluginType, CURR_TYPE);

        Class<?> sideParser = dtClassLoader.loadClass(className);
        if(!AbsSideTableParser.class.isAssignableFrom(sideParser)){
            throw new RuntimeException("class " + sideParser.getName() + " not subClass of AbsSideTableParser");
        }

        return sideParser.asSubclass(AbsTableParser.class).newInstance();
    }
}
