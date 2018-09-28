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

 

package com.dtstack.flink.sql.side.hbase;

import com.dtstack.flink.sql.side.hbase.enums.EReplaceType;

import java.io.Serializable;

/**
 * Reason:
 * Date: 2018/8/23
 * Company: www.dtstack.com
 *
 * @author xuchao
 */

public class ReplaceInfo implements Serializable {

    private static final long serialVersionUID = 2058635242957737717L;

    private EReplaceType type;

    private String param;

    public ReplaceInfo(EReplaceType type){
        this.type = type;
    }

    public EReplaceType getType() {
        return type;
    }

    public void setType(EReplaceType type) {
        this.type = type;
    }

    public String getParam() {
        return param;
    }

    public void setParam(String param) {
        this.param = param;
    }
}
