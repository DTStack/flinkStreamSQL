/*
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

package com.dtstack.flink.sql.dirtyManager.manager;

import com.dtstack.flink.sql.classloader.ClassLoaderManager;
import com.dtstack.flink.sql.dirtyManager.consumer.AbstractDirtyDataConsumer;
import com.dtstack.flink.sql.dirtyManager.entity.DirtyDataEntity;
import com.dtstack.flink.sql.util.PluginUtil;

import java.io.File;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author tiezhu
 * Company dtstack
 * Date 2020/8/27 星期四
 */
public class DirtyDataManager implements Serializable {
    private static final long serialVersionUID = 7190970299538893497L;

    private static final String CLASS_PRE_STR = "com.dtstack.flink.sql.dirty";

    private static final String CLASS_POST_STR = "DirtyDataConsumer";

    private static final String DIRTY_CONSUMER_PATH = "dirtyData";

    /**
     * 写入队列阻塞时间
     */
    private long blockingInterval = 60;

    /**
     * 缓存脏数据信息队列
     */
    private LinkedBlockingQueue<DirtyDataEntity> queue = new LinkedBlockingQueue<>();

    /**
     * 统计manager收集到的脏数据条数
     */
    private AtomicLong count = new AtomicLong(0);

    private AbstractDirtyDataConsumer consumer;

    public static DirtyDataManager newInstance() {

        return new DirtyDataManager();
    }

    public AbstractDirtyDataConsumer createConsumer(Map<String, String> properties) throws Exception {
        // 利用类加载的方式动态加载
        String type = properties.getOrDefault("type", "print");
        String consumerType = DIRTY_CONSUMER_PATH + File.separator + type;
        String consumerJar = PluginUtil.getJarFileDirPath(consumerType, properties.getOrDefault("pluginPath", null), "shipfile");
        String className = CLASS_PRE_STR + "." + type.toLowerCase() + "." + upperCaseFirstChar(type + CLASS_POST_STR);

        return ClassLoaderManager.newInstance(consumerJar, cl -> {
            Class<?> clazz = cl.loadClass(className);
            Constructor<?> constructor = clazz.getConstructor(String.class);
            return (AbstractDirtyDataConsumer) constructor.newInstance(type + CLASS_POST_STR);
        });
    }

    public void close() throws InterruptedException {
        if (!queue.isEmpty()) {
            flush();
        }
    }

    public void flush() throws InterruptedException {
        consumer.setQueue(queue);
        consumer.consume();
    }

    // 收集脏数据
    public void collectDirtyData(String dataInfo, String cause, String field) throws InterruptedException {
        DirtyDataEntity dirtyDataEntity = new DirtyDataEntity(dataInfo, System.currentTimeMillis(), cause, field);
        queue.offer(dirtyDataEntity, blockingInterval, TimeUnit.MILLISECONDS);
        count.incrementAndGet();
        consumer.setQueue(queue);
    }

    private static String upperCaseFirstChar(String str) {
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }
}
