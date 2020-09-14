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
import com.dtstack.flink.sql.factory.DTThreadFactory;
import com.dtstack.flink.sql.util.PluginUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author tiezhu
 * Company dtstack
 * Date 2020/8/27 星期四
 */
public class DirtyDataManager implements Serializable {

    private static final long serialVersionUID = 7190970299538893497L;

    private static final Logger LOG = LoggerFactory.getLogger(DirtyDataManager.class);

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
    public final LinkedBlockingQueue<DirtyDataEntity> queue = new LinkedBlockingQueue<>();

    /**
     * 统计manager收集到的脏数据条数
     */
    private final AtomicLong count = new AtomicLong(0);

    /**
     * 脏数据写入队列失败条数
     */
    private final AtomicLong errorCount = new AtomicLong(0);

    public static AbstractDirtyDataConsumer consumer;

    private static ThreadPoolExecutor dirtyDataConsumer;

    public final static int MAX_POOL_SIZE_LIMIT = 5;

    private final static int MAX_TASK_QUEUE_SIZE = 100;

    private final static String DEFAULT_TYPE = "console";

    /**
     * 通过参数生成manager实例，并同时将consumer实例化
     */
    public static DirtyDataManager newInstance(Map<String, String> properties) throws Exception {
        DirtyDataManager manager = new DirtyDataManager();
        manager.blockingInterval = Long.parseLong(properties.getOrDefault("blockingInterval", "60"));
        consumer = createConsumer(properties);
        consumer.init(properties);
        consumer.setQueue(manager.queue);
        dirtyDataConsumer = new ThreadPoolExecutor(MAX_POOL_SIZE_LIMIT, MAX_POOL_SIZE_LIMIT, 0, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(MAX_TASK_QUEUE_SIZE), new DTThreadFactory("dirtyDataConsumer"), new ThreadPoolExecutor.CallerRunsPolicy());
        dirtyDataConsumer.execute(consumer);

        return manager;
    }

    /**
     * 通过动态加载的方式加载Consumer
     */
    private static AbstractDirtyDataConsumer createConsumer(Map<String, String> properties) throws Exception {
        String type = properties.getOrDefault("type", DEFAULT_TYPE);
        String consumerType = DIRTY_CONSUMER_PATH + File.separator + type;
        String consumerJar = PluginUtil.getJarFileDirPath(consumerType, properties.getOrDefault("pluginPath", null), "shipfile");
        String className = CLASS_PRE_STR + "." + type.toLowerCase() + "." + upperCaseFirstChar(type + CLASS_POST_STR);

        return ClassLoaderManager.newInstance(consumerJar, cl -> {
            Class<?> clazz = cl.loadClass(className);
            Constructor<?> constructor = clazz.getConstructor();
            return (AbstractDirtyDataConsumer) constructor.newInstance();
        });
    }

    /**
     * 脏数据收集任务停止，任务停止之前，需要将队列中所有的数据清空
     */
    public void close() throws Exception {
        dirtyDataConsumer.shutdown();
        if (!queue.isEmpty() && checkConsumer()) {
            consumer.consume();
        }
        LOG.info("dirty consumer is closing ...");
        consumer.close();
    }

    /**
     * 收集脏数据放入队列缓存中，记录放入失败的数目和存入队列中的总数目，如果放入失败的数目超过一定比例，那么manager任务失败
     */
    public void collectDirtyData(String dataInfo, String cause, String field) {
        DirtyDataEntity dirtyDataEntity = new DirtyDataEntity(dataInfo, System.currentTimeMillis(), cause, field);
        try {
            queue.offer(dirtyDataEntity, blockingInterval, TimeUnit.MILLISECONDS);
            count.incrementAndGet();
        } catch (Exception ignored) {
            LOG.warn("dirty Data insert error ... Failed number: " + errorCount.incrementAndGet());
            LOG.warn("error dirty data:" + dirtyDataEntity.toString());
            // TODO 这个失败比例可以作出调整
            if (errorCount.get() > Math.ceil(count.longValue() * 0.8)) {
                throw new RuntimeException("The number of failed number reaches the limit, manager fails");
            }
        }
    }

    /**
     * 查看consumer当前状态
     */
    public boolean checkConsumer() {
        return consumer.isRunning();
    }

    /**
     * 首字母大写
     */
    private static String upperCaseFirstChar(String str) {
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }
}
