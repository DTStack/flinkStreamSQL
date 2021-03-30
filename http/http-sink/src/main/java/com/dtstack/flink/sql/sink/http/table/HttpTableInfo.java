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

package com.dtstack.flink.sql.sink.http.table;

import avro.shaded.com.google.common.base.Preconditions;
import com.dtstack.flink.sql.table.AbstractTargetTableInfo;

/**
 * @author: chuixue
 * @create: 2021-03-03 10:41
 * @description:
 **/
public class HttpTableInfo extends AbstractTargetTableInfo {

    private static final String CURR_TYPE = "http";

    public static final String URL_KEY = "url";

    public static final String DELAY_KEY = "delay";

    public static final String FLAG_KEY = "flag";

    public static final String MAXNUMRETRIES_KEY = "maxNumRetries";

    public HttpTableInfo() {
        super.setType(CURR_TYPE);
    }

    private String url;

    private int delay;

    private String flag;

    private long maxNumRetries;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public int getDelay() {
        return delay;
    }

    public void setDelay(int delay) {
        this.delay = delay;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public long getMaxNumRetries() {
        return maxNumRetries;
    }

    public void setMaxNumRetries(long maxNumRetries) {
        this.maxNumRetries = maxNumRetries;
    }

    @Override
    public boolean check() {
        Preconditions.checkNotNull(url, "url not null");
        Preconditions.checkNotNull(maxNumRetries, "maxNumRetries name not null");

        Preconditions.checkArgument(maxNumRetries >= -1, "maxNumRetries must be zero or larger (num retries), or -1 (infinite retries)");
        return false;
    }
}
