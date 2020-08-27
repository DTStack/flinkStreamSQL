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

package com.dtstack.flink.sql.dirty.manager;

import com.dtstack.flink.sql.dirty.consumer.AbstractDirtyDataConsumer;
import com.dtstack.flink.sql.dirty.entity.DirtyDataEntity;

import java.io.Serializable;
import java.util.Map;
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
        return null;
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

}
