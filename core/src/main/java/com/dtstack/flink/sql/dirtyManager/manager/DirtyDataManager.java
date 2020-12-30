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

import com.alibaba.fastjson.JSONObject;
import com.dtstack.flink.sql.dirtyManager.consumer.AbstractDirtyDataConsumer;
import com.dtstack.flink.sql.dirtyManager.consumer.DirtyConsumerFactory;
import com.dtstack.flink.sql.dirtyManager.entity.DirtyDataEntity;
import com.dtstack.flink.sql.factory.DTThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Properties;
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

    public final static int MAX_POOL_SIZE_LIMIT = 5;
    private static final long serialVersionUID = 7190970299538893497L;
    private static final Logger LOG = LoggerFactory.getLogger(DirtyDataManager.class);
    private static final String DIRTY_BLOCK_STR = "blockingInterval";
    private static final String DIRTY_LIMIT_RATE_STR = "errorLimitRate";
    private final static int MAX_TASK_QUEUE_SIZE = 100;
    private final static String DEFAULT_ERROR_LIMIT_RATE = "0.8";
    private final static String DEFAULT_BLOCKING_INTERVAL = "60";
    public static AbstractDirtyDataConsumer consumer;

    private static ThreadPoolExecutor dirtyDataConsumer;
    /**
     * 统计manager收集到的脏数据条数
     */
    private final AtomicLong count = new AtomicLong(0);
    /**
     * 脏数据写入队列失败条数
     */
    private final AtomicLong errorCount = new AtomicLong(0);
    /**
     * 写入队列阻塞时间
     */
    private long blockingInterval;
    /**
     * 任务失败的脏数据比例
     */
    private double errorLimitRate;

    /**
     * 通过参数生成manager实例，并同时将consumer实例化
     */
    public static DirtyDataManager newInstance(Properties properties) {
        try {
            DirtyDataManager manager = new DirtyDataManager();
            manager.blockingInterval = Long.parseLong(String.valueOf(properties.getOrDefault(DIRTY_BLOCK_STR, DEFAULT_BLOCKING_INTERVAL)));
            manager.errorLimitRate = Double.parseDouble(String.valueOf(properties.getOrDefault(DIRTY_LIMIT_RATE_STR, DEFAULT_ERROR_LIMIT_RATE)));
            consumer = DirtyConsumerFactory.getDirtyConsumer(
                    properties.getProperty("type")
                    , properties.getProperty("pluginPath")
                    , properties.getProperty("pluginLoadMode")
            );
            consumer.init(properties);
            consumer.setQueue(new LinkedBlockingQueue<>());
            dirtyDataConsumer = new ThreadPoolExecutor(MAX_POOL_SIZE_LIMIT, MAX_POOL_SIZE_LIMIT, 0, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(MAX_TASK_QUEUE_SIZE), new DTThreadFactory("dirtyDataConsumer"), new ThreadPoolExecutor.CallerRunsPolicy());
            dirtyDataConsumer.execute(consumer);
            return manager;
        } catch (Exception e) {
            throw new RuntimeException("create dirtyManager error!", e);
        }
    }

    /**
     * 设置脏数据插件默认配置
     *
     * @return console的默认配置
     */
    public static String buildDefaultDirty() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("type", "console");
        jsonObject.put("printLimit", "1000");
        return jsonObject.toJSONString();
    }

    /**
     * 脏数据收集任务停止，任务停止之前，需要将队列中所有的数据清空
     * TODO consumer 关闭时仍有数据没有消费到，假如有500条数据，在结束时实际消费数量可能只有493
     */
    public void close() {
        if (checkConsumer()) {
            LOG.info("dirty consumer is closing ...");
            consumer.close();
            dirtyDataConsumer.shutdownNow();
        }
    }

    /**
     * 收集脏数据放入队列缓存中，记录放入失败的数目和存入队列中的总数目，如果放入失败的数目超过一定比例，那么manager任务失败
     */
    public void collectDirtyData(String dataInfo, String cause) {
        DirtyDataEntity dirtyDataEntity = new DirtyDataEntity(dataInfo, System.currentTimeMillis(), cause);
        try {
            consumer.collectDirtyData(dirtyDataEntity, blockingInterval);
            count.incrementAndGet();
        } catch (Exception ignored) {
            LOG.warn("dirty Data insert error ... Failed number: " + errorCount.incrementAndGet());
            LOG.warn("error dirty data:" + dirtyDataEntity.toString());
            if (errorCount.get() > Math.ceil(count.longValue() * errorLimitRate)) {
                throw new RuntimeException(String.format("The number of failed number 【%s】 reaches the limit, manager fails", errorCount.get()));
            }
        }
    }

    /**
     * 查看consumer当前状态
     */
    public boolean checkConsumer() {
        return consumer.isRunning();
    }
}
