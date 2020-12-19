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
import com.dtstack.flink.sql.util.ReflectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * company: www.dtstack.com
 * author: toutian
 * create: 2019/10/14
 */
public class ClassLoaderManager {

    private static final Logger LOG = LoggerFactory.getLogger(ClassLoaderManager.class);

    private static Map<String, DtClassLoader> pluginClassLoader = new ConcurrentHashMap<>();
    // 维表插件的classloader，该集合存放的是AppClassloader
    private static Map<String, URLClassLoader> pluginAppClassLoader = new ConcurrentHashMap<>();

    public static <R> R newInstance(String pluginJarPath, ClassLoaderSupplier<R> supplier) throws Exception {
        ClassLoader classLoader = retrieveClassLoad(pluginJarPath);
        return ClassLoaderSupplierCallBack.callbackAndReset(supplier, classLoader);
    }

    /**
     * 使用AppClassloader实例化维表插件
     * @param pluginJarPath 插件路径
     * @param supplier 类加载器
     * @param <R>
     * @return
     * @throws Exception
     */
    public static <R> R newAppInstance(String pluginJarPath, ClassLoaderSupplier<R> supplier) throws Exception {
        ClassLoader classLoader = retrieveAppClassLoad(pluginJarPath);
        return ClassLoaderSupplierCallBack.callbackAndReset(supplier, classLoader);
    }

    public static <R> R newInstance(List<URL> jarUrls, ClassLoaderSupplier<R> supplier) throws Exception {
        ClassLoader classLoader = retrieveClassLoad(jarUrls);
        return ClassLoaderSupplierCallBack.callbackAndReset(supplier, classLoader);
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
                LOG.error("retrieve ClassLoad happens error", e);
                throw new RuntimeException("retrieve ClassLoad happens error");
            }
        });
    }

    /**
     * 使用AppClassloader加载维表插件
     * @param pluginJarPath 插件路径
     * @return
     */
    private static URLClassLoader retrieveAppClassLoad(String pluginJarPath) {
        return pluginAppClassLoader.computeIfAbsent(pluginJarPath, k -> {
            try {
                URL[] urls = PluginUtil.getPluginJarUrls(pluginJarPath);
                ClassLoader parentClassLoader = Thread.currentThread().getContextClassLoader();
                List<URL> jarUrlList = Arrays.asList(urls);
                URLClassLoader classLoader = loadExtraJar(jarUrlList, (URLClassLoader) parentClassLoader);
                LOG.info("pluginJarPath:{} create ClassLoad successful...", pluginJarPath);
                return classLoader;
            } catch (Throwable e) {
                LOG.error("retrieve ClassLoad happens error", e);
                throw new RuntimeException("retrieve ClassLoad happens error");
            }
        });
    }

    private static DtClassLoader retrieveClassLoad(List<URL> jarUrls) {
        jarUrls.sort(Comparator.comparing(URL::toString));
        String jarUrlkey = StringUtils.join(jarUrls, "_");
        return pluginClassLoader.computeIfAbsent(jarUrlkey, k -> {
            try {
                URL[] urls = jarUrls.toArray(new URL[jarUrls.size()]);
                ClassLoader parentClassLoader = Thread.currentThread().getContextClassLoader();
                DtClassLoader classLoader = new DtClassLoader(urls, parentClassLoader);
                LOG.info("jarUrl:{} create ClassLoad successful...", jarUrlkey);
                return classLoader;
            } catch (Throwable e) {
                LOG.error("retrieve ClassLoad happens error:{}", e);
                throw new RuntimeException("retrieve ClassLoad happens error");
            }
        });
    }

    public static List<URL> getClassPath() {
        List<URL> classPaths = new ArrayList<>();
        for (Map.Entry<String, DtClassLoader> entry : pluginClassLoader.entrySet()) {
            classPaths.addAll(Arrays.asList(entry.getValue().getURLs()));
        }

        Set<URL> appClassPaths = new HashSet<URL>();
        for(Map.Entry<String, URLClassLoader> entry : pluginAppClassLoader.entrySet()){
            appClassPaths.addAll(Arrays.asList(entry.getValue().getURLs()));
        }
        classPaths.addAll(appClassPaths);
        return classPaths;
    }



    public static URLClassLoader loadExtraJar(List<URL> jarUrlList, URLClassLoader classLoader)
            throws  IllegalAccessException, InvocationTargetException {
        for(URL url : jarUrlList){
            if(url.toString().endsWith(".jar")){
                urlClassLoaderAddUrl(classLoader, url);
            }
        }
        return classLoader;
    }

    private static void urlClassLoaderAddUrl(URLClassLoader classLoader, URL url) throws InvocationTargetException, IllegalAccessException {
        Method method = ReflectionUtils.getDeclaredMethod(classLoader, "addURL", URL.class);

        if (method == null) {
            throw new RuntimeException("can't not find declared method addURL, curr classLoader is " + classLoader.getClass());
        }
        method.setAccessible(true);
        method.invoke(classLoader, url);
    }
}
