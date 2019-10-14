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

package com.dtstack.flink.sql.classloader;

import com.dtstack.flink.sql.util.PluginUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * company: www.dtstack.com
 * author: toutian
 * create: 2019/10/14
 */
public class ClassLoaderManager {

    private static final Logger LOG = LoggerFactory.getLogger(ClassLoaderManager.class);

    private static Map<String, DtClassLoader> pluginClassLoader = new ConcurrentHashMap<>();

    public static <R> R newInstance(String pluginJarPath, DtSupplier<R> supplier) throws Exception {
        ClassLoader classLoader = retrieveClassLoad(pluginJarPath);
        return ClassLoaderCallBackMethod.callbackAndReset(supplier, classLoader);
    }

    private static DtClassLoader retrieveClassLoad(String pluginJarPath) {
        return pluginClassLoader.computeIfAbsent(pluginJarPath, k -> {
            try {
                URL[] urls = PluginUtil.getPluginJarUrls(pluginJarPath);
                ClassLoader parentClassLoader = Thread.currentThread().getContextClassLoader();
                DtClassLoader classLoader = new DtClassLoader(urls, parentClassLoader);
                LOG.info("pluginJarPath:{} create ClassLoad successful...", pluginJarPath);
                return classLoader;
            } catch (Throwable e) {
                LOG.error("retrieve ClassLoad happens error:{}", e);
                throw new RuntimeException("retrieve ClassLoad happens error");
            }
        });
    }
}
