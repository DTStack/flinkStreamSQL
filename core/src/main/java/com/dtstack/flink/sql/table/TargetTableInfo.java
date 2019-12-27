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

 

package com.dtstack.flink.sql.table;

/**
 * Reason:
 * Date: 2018/6/25
 * Company: www.dtstack.com
 * @author xuchao
 */

public abstract class TargetTableInfo extends TableInfo {

    public static final String TARGET_SUFFIX = "Sink";

    public static final String SINK_DATA_TYPE = "sinkdatatype";

    private String sinkDataType = "json";

    private String isExactlyOnce;

    public String getSinkDataType() {
        return sinkDataType;
    }

    public void setSinkDataType(String sinkDataType) {
        this.sinkDataType = sinkDataType;
    }

    public String getIsExactlyOnce() {
        return isExactlyOnce;
    }

    public void setIsExactlyOnce(String isExactlyOnce) {
        this.isExactlyOnce = isExactlyOnce;
    }

}
