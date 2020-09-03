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

package com.dtstack.flink.sql.dirty.print;

import com.dtstack.flink.sql.dirtyManager.consumer.AbstractDirtyDataConsumer;
import com.dtstack.flink.sql.dirtyManager.entity.DirtyDataEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author tiezhu
 * Company dtstack
 * Date 2020/8/27 星期四
 */
public class PrintDirtyDataConsumer extends AbstractDirtyDataConsumer {
    private static final long serialVersionUID = 5727194679865135189L;

    private final Logger LOG = LoggerFactory.getLogger(PrintDirtyDataConsumer.class);

    @Override
    public void consume() throws InterruptedException {
        DirtyDataEntity dataEntity = queue.take();
        count++;
        LOG.warn("get dirtyData: " + dataEntity.getDirtyData() + "\n"
                + "cause: " + dataEntity.getCause() + "\n"
                + "processTime: " + dataEntity.getProcessDate() + "\n"
                + "error field: " + dataEntity.getField());
    }

    @Override
    public void close() {
        isRunning.compareAndSet(true, false);
    }

    @Override
    public void init(Map<String, String> properties) {
        LOG.info("Log dirty consumer init ......");
    }
}
