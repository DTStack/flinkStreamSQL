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

package com.dtstack.flink.sql.dirty.console;

import com.dtstack.flink.sql.dirtyManager.consumer.AbstractDirtyDataConsumer;
import com.dtstack.flink.sql.dirtyManager.entity.DirtyDataEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;

/**
 * @author tiezhu
 * Company dtstack
 * Date 2020/8/27 星期四
 */
public class ConsoleDirtyDataConsumer extends AbstractDirtyDataConsumer {
    private static final long serialVersionUID = 5727194679865135189L;

    private static final Logger LOG = LoggerFactory.getLogger(ConsoleDirtyDataConsumer.class);

    private static final Long DEFAULT_PRINT_LIMIT = 1000L;

    private Long printLimit;

    @Override
    public void consume() throws InterruptedException {
        DirtyDataEntity dataEntity = queue.take();
        if (count.getAndIncrement() % printLimit == 0) {
            LOG.warn("\nget dirtyData: " + dataEntity.getDirtyData() + "\n"
                    + "cause: " + dataEntity.getCause() + "\n"
                    + "processTime: " + dataEntity.getProcessDate() + "\n"
                    + "error field: " + dataEntity.getField());
        }
    }

    @Override
    public void close() {
        isRunning.compareAndSet(true, false);
        LOG.info("console dirty consumer close ...");
    }

    @Override
    public void init(Properties properties) {
        LOG.info("console dirty consumer init ...");
        Object printLimit = properties.get("printLimit");
        this.printLimit = Objects.isNull(printLimit) ?
                DEFAULT_PRINT_LIMIT : Long.parseLong(String.valueOf(printLimit));
    }
}
