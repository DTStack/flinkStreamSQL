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

package com.dtstack.flink.sql.side.table;

import com.dtstack.flink.sql.classloader.ClassLoaderManager;
import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.util.PluginUtil;

/**
 * @author: chuixue
 * @create: 2020-10-12 09:54
 * @description:创建全量/异步维表函数类的工厂
 **/
public class LookupTableFunctionFactory {
    // 全量维表
    private static final String ALL_PATH_FORMAT = "%sallside";
    // 异步维表
    private static final String LRU_PATH_FORMAT = "%sasyncside";
    /**
     * 创建全量维表
     *
     * @param abstractSideTableInfo 表信息
     * @param sqlRootDir            插件路径
     * @param pluginLoadMode        插件模式
     * @param lookupKeys            关联字段
     * @return
     */
    public static BaseTableFunction createLookupTableFunction(AbstractSideTableInfo abstractSideTableInfo
            , String sqlRootDir
            , String pluginLoadMode
            , String[] lookupKeys) throws Exception {

        String pathOfType = String.format(ALL_PATH_FORMAT, abstractSideTableInfo.getType());
        String pluginJarPath = PluginUtil.getJarFileDirPath(pathOfType, sqlRootDir, pluginLoadMode);
        String className = PluginUtil.getTableFunctionClassName(abstractSideTableInfo.getType(), "side");

        return ClassLoaderManager.newAppInstance(pluginJarPath,
                (cl) -> cl.loadClass(className).asSubclass(BaseTableFunction.class)
                        .getConstructor(AbstractSideTableInfo.class, String[].class)
                        .newInstance(abstractSideTableInfo, lookupKeys));
    }


    /**
     * 创建异步维表
     *
     * @param abstractSideTableInfo 表信息
     * @param sqlRootDir            插件路径
     * @param pluginLoadMode        插件模式
     * @param lookupKeys            关联字段
     * @return
     */
    public static BaseAsyncTableFunction createLookupAsyncTableFunction(AbstractSideTableInfo abstractSideTableInfo
            , String sqlRootDir
            , String pluginLoadMode
            , String[] lookupKeys) throws Exception {
        String pathOfType = String.format(LRU_PATH_FORMAT, abstractSideTableInfo.getType());
        String pluginJarPath = PluginUtil.getJarFileDirPath(pathOfType, sqlRootDir, pluginLoadMode);
        String className = PluginUtil.getAsyncTableFunctionClassName(abstractSideTableInfo.getType(), "side");

        return ClassLoaderManager.newAppInstance(pluginJarPath,
                (cl) -> cl.loadClass(className).asSubclass(BaseAsyncTableFunction.class)
                        .getConstructor(AbstractSideTableInfo.class, String[].class)
                        .newInstance(abstractSideTableInfo, lookupKeys));
    }
}
