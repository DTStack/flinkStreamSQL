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

 

package com.dtstack.flink.sql.sink;

import com.dtstack.flink.sql.classloader.ClassLoaderManager;
import com.dtstack.flink.sql.table.AbstractTableParser;
import com.dtstack.flink.sql.table.AbstractTargetTableInfo;
import com.dtstack.flink.sql.util.DtStringUtil;
import com.dtstack.flink.sql.util.PluginUtil;
import org.apache.flink.table.sinks.TableSink;

/**
 * Loads jar and initializes the object according to the specified sink type
 * Date: 2017/3/10
 * Company: www.dtstack.com
 * @author xuchao
 */

public class StreamSinkFactory {

    public static String CURR_TYPE = "sink";

    private static final String DIR_NAME_FORMAT = "%ssink";

    public static AbstractTableParser getSqlParser(String pluginType, String sqlRootDir) throws Exception {
        String pluginJarPath = PluginUtil.getJarFileDirPath(String.format(DIR_NAME_FORMAT, pluginType), sqlRootDir);
        String typeNoVersion = DtStringUtil.getPluginTypeWithoutVersion(pluginType);
        String className = PluginUtil.getSqlParserClassName(typeNoVersion, CURR_TYPE);

        return ClassLoaderManager.newInstance(pluginJarPath, (cl) -> {
            Class<?> targetParser = cl.loadClass(className);
            if(!AbstractTableParser.class.isAssignableFrom(targetParser)){
                throw new RuntimeException("class " + targetParser.getName() + " not subClass of AbsTableParser");
            }
            return targetParser.asSubclass(AbstractTableParser.class).newInstance();
        });
    }

    public static TableSink getTableSink(AbstractTargetTableInfo targetTableInfo, String localSqlRootDir) throws Exception {
        String pluginType = targetTableInfo.getType();
        String pluginJarDirPath = PluginUtil.getJarFileDirPath(String.format(DIR_NAME_FORMAT, pluginType), localSqlRootDir);
        String typeNoVersion = DtStringUtil.getPluginTypeWithoutVersion(pluginType);
        String className = PluginUtil.getGenerClassName(typeNoVersion, CURR_TYPE);

        return ClassLoaderManager.newInstance(pluginJarDirPath, (cl) -> {
            Class<?> sinkClass = cl.loadClass(className);
            if(!IStreamSinkGener.class.isAssignableFrom(sinkClass)){
                throw new RuntimeException("class " + sinkClass + " not subClass of IStreamSinkGener");
            }
            IStreamSinkGener streamSinkGener = sinkClass.asSubclass(IStreamSinkGener.class).newInstance();
            Object result = streamSinkGener.genStreamSink(targetTableInfo);
            return (TableSink) result;
        });
    }
}
