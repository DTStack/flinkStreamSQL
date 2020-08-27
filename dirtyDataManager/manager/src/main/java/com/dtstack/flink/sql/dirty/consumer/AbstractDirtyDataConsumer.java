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

package com.dtstack.flink.sql.dirty.consumer;

import com.dtstack.flink.sql.dirty.entity.DirtyDataEntity;

import java.io.Serializable;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author tiezhu
 * Company dtstack
 * Date 2020/8/27 星期四
 */
public abstract class AbstractDirtyDataConsumer extends Thread implements  Serializable {
    private static final long serialVersionUID = -6058598201315176687L;

    private long count = 0;

    protected LinkedBlockingQueue<DirtyDataEntity> queue;

    /**
     * 消费队列数据
     */
    public abstract void consume() throws InterruptedException;

    /**
     * 关闭消费者，需要释放资源
     */
    public abstract void close();

    /**
     * 初始化消费者，初始化定时任务
     */
    public void init() {

    }

    @Override
    public void run() {

    }

    public void setQueue(LinkedBlockingQueue<DirtyDataEntity> queue) {
        this.queue = queue;
    }
}
