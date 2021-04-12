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
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.dtstack.flink.sql.dirtyManager.manager.DirtyKeys.DEFAULT_BLOCKING_INTERVAL;
import static com.dtstack.flink.sql.dirtyManager.manager.DirtyKeys.DEFAULT_ERROR_LIMIT_RATE;
import static com.dtstack.flink.sql.dirtyManager.manager.DirtyKeys.DEFAULT_PRINT_LIMIT;
import static com.dtstack.flink.sql.dirtyManager.manager.DirtyKeys.DEFAULT_TYPE;
import static com.dtstack.flink.sql.dirtyManager.manager.DirtyKeys.DIRTY_BLOCK_STR;
import static com.dtstack.flink.sql.dirtyManager.manager.DirtyKeys.DIRTY_LIMIT_RATE_STR;
import static com.dtstack.flink.sql.dirtyManager.manager.DirtyKeys.PLUGIN_LOAD_MODE_STR;
import static com.dtstack.flink.sql.dirtyManager.manager.DirtyKeys.PLUGIN_PATH_STR;
import static com.dtstack.flink.sql.dirtyManager.manager.DirtyKeys.PLUGIN_TYPE_STR;
import static com.dtstack.flink.sql.dirtyManager.manager.DirtyKeys.PRINT_LIMIT_STR;

/**
 * @author tiezhu
 * Company dtstack
 * Date 2020/8/27 星期四
 */
public class DirtyDataManager implements Serializable {
    private static final long serialVersionUID = 1L;

    public final static int MAX_POOL_SIZE_LIMIT = 5;
    private static final Logger LOG = LoggerFactory.getLogger(DirtyDataManager.class);
    private final static int MAX_TASK_QUEUE_SIZE = 100;

    private AbstractDirtyDataConsumer consumer;
    private transient ThreadPoolExecutor dirtyDataConsumer;

    private static final DirtyDataManager INSTANCE = new DirtyDataManager();

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

    private DirtyDataManager() {

    }

    /**
     * 通过参数生成manager实例，并同时将consumer实例化
     */
    public static DirtyDataManager newInstance(Map<String, Object> properties) {
        try {
            INSTANCE.setBlockingInterval(Long.parseLong(
                    String.valueOf(properties.getOrDefault(DIRTY_BLOCK_STR, DEFAULT_BLOCKING_INTERVAL))));
            INSTANCE.setErrorLimitRate(Double.parseDouble(
                    String.valueOf(properties.getOrDefault(DIRTY_LIMIT_RATE_STR, DEFAULT_ERROR_LIMIT_RATE))));

            INSTANCE.setConsumer(properties);
            return INSTANCE;
        } catch (Exception e) {
            throw new RuntimeException("create dirtyManager error!", e);
        }
    }

    private void setConsumer(Map<String, Object> properties) throws Exception {
        consumer = DirtyConsumerFactory.getDirtyConsumer(
                String.valueOf(properties.getOrDefault(PLUGIN_TYPE_STR, DEFAULT_TYPE)),
                String.valueOf(properties.get(PLUGIN_PATH_STR)),
                String.valueOf(properties.get(PLUGIN_LOAD_MODE_STR))
        );
        consumer.init(properties);
        consumer.setQueue(new LinkedBlockingQueue<>());
    }

    public void execute() {
        if (Objects.isNull(dirtyDataConsumer)) {
            dirtyDataConsumer = new ThreadPoolExecutor(
                    MAX_POOL_SIZE_LIMIT,
                    MAX_POOL_SIZE_LIMIT,
                    0,
                    TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(MAX_TASK_QUEUE_SIZE),
                    new DTThreadFactory("dirtyDataConsumer", true),
                    new ThreadPoolExecutor.CallerRunsPolicy());
            dirtyDataConsumer.execute(consumer);
        }
    }

    /**
     * 设置脏数据插件默认配置
     *
     * @return console的默认配置
     */
    public static String buildDefaultDirty() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(PLUGIN_TYPE_STR, DEFAULT_TYPE);
        jsonObject.put(PRINT_LIMIT_STR, DEFAULT_PRINT_LIMIT);
        return jsonObject.toJSONString();
    }

    /**
     * 脏数据收集任务停止，任务停止之前，需要将队列中所有的数据清空
     */
    public void close() {
        if (consumer != null && checkConsumer()) {
            LOG.info("dirty consumer is closing ...");
            consumer.close();
        }

        if (dirtyDataConsumer != null) {
            dirtyDataConsumer.shutdownNow();
        }
    }

    /**
     * 收集脏数据放入队列缓存中，记录放入失败的数目和存入队列中的总数目，如果放入失败的数目超过一定比例，那么manager任务失败
     */
    public void collectDirtyData(String dataInfo, String cause) {
        DirtyDataEntity dirtyDataEntity = new DirtyDataEntity(dataInfo, System.currentTimeMillis(), cause);
        try {
            count.incrementAndGet();
            consumer.collectDirtyData(dirtyDataEntity, blockingInterval);
        } catch (Exception e) {
            LOG.warn("dirty Data insert error ... Failed number: " + errorCount.incrementAndGet());
            LOG.warn("error cause: " + e.getMessage());
            LOG.warn("error dirty data:" + dirtyDataEntity.getDirtyData());
            if (errorCount.get() > Math.ceil(count.longValue() * errorLimitRate)) {
                // close consumer and manager
                close();
                throw new RuntimeException(
                        String.format("The number of failed number 【%s】 reaches the limit, manager fails", errorCount.get()));
            }
        }
    }

    /**
     * 查看consumer当前状态
     */
    public boolean checkConsumer() {
        return consumer.isRunning();
    }

    public AtomicLong getCount() {
        return count;
    }

    public AtomicLong getErrorCount() {
        return errorCount;
    }

    public long getBlockingInterval() {
        return blockingInterval;
    }

    public void setBlockingInterval(long blockingInterval) {
        this.blockingInterval = blockingInterval;
    }

    public double getErrorLimitRate() {
        return errorLimitRate;
    }

    public void setErrorLimitRate(double errorLimitRate) {
        this.errorLimitRate = errorLimitRate;
    }
}
