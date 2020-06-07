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



package com.dtstack.flink.sql.util;

import io.vertx.core.Future;
import io.vertx.core.Vertx;

/**
 * Date: 2020/06/07
 * Company: abakus
 * @author chenxyz
 */

public class VertxUtils {

    public static final long DEFAULT_TIME_OUT = 3_000L;

    public static void asyClose(Vertx vertx) {
        if (vertx != null) {
            vertx.close();
        }
    }


    /**
     * 一种阻塞的关闭方式
     * 由于Vertx#close的关闭是异步的，可能导致主线程运行完毕，classloader释放了加载的class
     * 出现class无法找到的问题
     * @param vertx
     */
    public static void synClose(Vertx vertx) {
        if (vertx != null) {
            synClose(vertx, DEFAULT_TIME_OUT);
        }
    }


    public static void synClose(Vertx vertx, Long timeout) {
        if (timeout <= 0) {
            timeout = DEFAULT_TIME_OUT;
        }
        if (vertx != null) {
            long start = System.currentTimeMillis();
            Future<Void> future = Future.future();
            vertx.close(future);
            for (;;) {
                if (future.isComplete() || System.currentTimeMillis() - start >= timeout) {
                    return;
                }
            }
        }
    }
}
