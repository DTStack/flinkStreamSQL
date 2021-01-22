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

package com.dtstack.flink.sql.dirtyManager.consumer;

import com.dtstack.flink.sql.classloader.ClassLoaderManager;
import com.dtstack.flink.sql.util.PluginUtil;

import java.io.File;
import java.lang.reflect.Constructor;
import java.util.Objects;

import static com.dtstack.flink.sql.util.PluginUtil.upperCaseFirstChar;

/**
 * @author tiezhu
 * Company dtstack
 * Date 2020/12/21 星期一
 */
public class DirtyConsumerFactory {
    public static final String DEFAULT_DIRTY_TYPE = "console";
    public static final String DIRTY_CONSUMER_PATH = "dirtyData";
    public static final String CLASS_PRE_STR = "com.dtstack.flink.sql.dirty";
    public static final String CLASS_POST_STR = "DirtyDataConsumer";

    /**
     * 通过动态方式去加载脏数据插件
     *
     * @param dirtyType      脏数据插件类型
     * @param pluginPath     脏数据插件直地址
     * @param pluginLoadMode 插件加载方式
     * @return 脏数据消费者
     * @throws Exception exception
     */
    public static AbstractDirtyDataConsumer getDirtyConsumer(
            String dirtyType
            , String pluginPath
            , String pluginLoadMode) throws Exception {
        if (Objects.isNull(dirtyType)) {
            dirtyType = DEFAULT_DIRTY_TYPE;
        }
        String consumerType = DIRTY_CONSUMER_PATH + File.separator + dirtyType;
        String consumerJar = PluginUtil.getJarFileDirPath(consumerType, pluginPath, pluginLoadMode);
        String className = CLASS_PRE_STR + "." + dirtyType.toLowerCase() + "." + upperCaseFirstChar(dirtyType + CLASS_POST_STR);
        return ClassLoaderManager.newInstance(consumerJar, cl -> {
            Class<?> clazz = cl.loadClass(className);
            Constructor<?> constructor = clazz.getConstructor();
            return (AbstractDirtyDataConsumer) constructor.newInstance();
        });
    }
}
