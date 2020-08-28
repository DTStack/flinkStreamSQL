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

package com.dtstack.flink.sql.dirtyManager.entity;

import java.sql.Date;

/**
 * @author tiezhu
 * Company dtstack
 * Date 2020/8/27 星期四
 */
public class DirtyDataEntity {
    /**
     * 脏数据信息内容
     */
    private String dirtyData;

    /**
     * 脏数据处理时间
     */
    private Date processDate;

    /**
     * 产生脏数据的原因
     */
    private String cause;

    /**
     * 产生异常的字段
     */
    private String field;

    public String getDirtyData() {
        return dirtyData;
    }

    public void setDirtyData(String dirtyData) {
        this.dirtyData = dirtyData;
    }

    public Date getProcessDate() {
        return processDate;
    }

    public void setProcessDate(Date processDate) {
        this.processDate = processDate;
    }

    public String getCause() {
        return cause;
    }

    public void setCause(String cause) {
        this.cause = cause;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public DirtyDataEntity(String dirtyData, Long processDate, String cause, String field) {
        this.dirtyData = dirtyData;
        this.processDate = new Date(processDate);
        this.cause = cause;
        this.field = field;
    }
}
