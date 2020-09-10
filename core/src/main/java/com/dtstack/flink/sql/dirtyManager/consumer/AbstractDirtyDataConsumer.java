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

package com.dtstack.flink.sql.dirtyManager.consumer;

import com.dtstack.flink.sql.dirtyManager.entity.DirtyDataEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author tiezhu
 * Company dtstack
 * Date 2020/8/27 星期四
 */
public abstract class AbstractDirtyDataConsumer implements Runnable, Serializable {
    protected static final long serialVersionUID = -6058598201315176687L;

    protected static final Logger LOG = LoggerFactory.getLogger(AbstractDirtyDataConsumer.class);

    protected long errorLimit = 1000;
    protected long errorCount = 0;

    protected long count = 0;

    public AtomicBoolean isRunning = new AtomicBoolean(true);

    protected LinkedBlockingQueue<DirtyDataEntity> queue;

    /**
     * 消费队列数据
     */
    public abstract void consume() throws Exception;

    /**
     * 关闭消费者，需要释放资源
     */
    public abstract void close();

    /**
     * 初始化消费者，初始化定时任务
     */
    public abstract void init(Map<String, String> properties) throws Exception;

    /**
     * 检验consumer是否正在执行
     */
    public boolean isRunning() {
        return isRunning.get();
    }

    @Override
    public void run() {
        try {
            LOG.info("start to consume dirty data");
            while (isRunning.get()) {
                consume();
            }
            LOG.info("consume dirty data end");
        } catch (Exception e) {
            LOG.error("consume dirtyData error");
            errorCount++;
            if (errorCount == errorLimit) {
                throw new RuntimeException("脏数据消费失败达到上限，任务失败");
            }
        }
    }

    public AbstractDirtyDataConsumer setQueue(LinkedBlockingQueue<DirtyDataEntity> queue) {
        this.queue = queue;
        return this;
    }
}
