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

package com.dtstack.flink.sql.sink.solr.options;

import java.io.Serializable;
import java.util.Optional;

/**
 * @author wuren
 * @program flinkStreamSQL
 * @create 2021/05/17
 */
public class SolrWriteOptions implements Serializable {

    private static final long DEFAULT_BUFFER_FLUSH_INTERVAL_MILLIS = 1000;
    private static final int DEFAULT_BUFFER_FLUSH_MAX_ROWS = 1000;
    private static final int DEFAULT_PARALLELISMS = -1;
    private final long bufferFlushIntervalMillis;
    private final int bufferFlushMaxRows;
    private final Integer parallelism;

    public SolrWriteOptions(
            Optional<Long> bufferFlushIntervalMillis,
            Optional<Integer> bufferFlushMaxRows,
            Optional<Integer> parallelism) {
        this.bufferFlushIntervalMillis =
                bufferFlushIntervalMillis.orElse(DEFAULT_BUFFER_FLUSH_INTERVAL_MILLIS);
        this.bufferFlushMaxRows = bufferFlushMaxRows.orElse(DEFAULT_BUFFER_FLUSH_MAX_ROWS);
        this.parallelism = parallelism.orElse(DEFAULT_PARALLELISMS);
    }

    public int getParallelism() {
        return parallelism;
    }

    public long getBufferFlushIntervalMillis() {
        return bufferFlushIntervalMillis;
    }

    public int getBufferFlushMaxRows() {
        return bufferFlushMaxRows;
    }
}
