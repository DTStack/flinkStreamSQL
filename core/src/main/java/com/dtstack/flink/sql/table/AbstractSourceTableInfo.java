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

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Reason:
 * Date: 2018/6/25
 * Company: www.dtstack.com
 * @author xuchao
 */

public abstract class AbstractSourceTableInfo extends AbstractTableInfo {

    public static final String SOURCE_SUFFIX = "Source";

    private String eventTimeField;

    private Integer maxOutOrderness = 10;

    private Map<String, String> virtualFields = Maps.newHashMap();

    @Override
    public boolean check() {
       return true;
    }

    public String getEventTimeField() {
        return eventTimeField;
    }

    public void setEventTimeField(String eventTimeField) {
        this.eventTimeField = eventTimeField;
    }

    public int getMaxOutOrderness() {
        return maxOutOrderness;
    }

    public void setMaxOutOrderness(Integer maxOutOrderness) {
        if(maxOutOrderness == null){
            return;
        }

        this.maxOutOrderness = maxOutOrderness;
    }

    public Map<String, String> getVirtualFields() {
        return virtualFields;
    }

    public void setVirtualFields(Map<String, String> virtualFields) {
        this.virtualFields = virtualFields;
    }

    public void addVirtualField(String fieldName, String expression){
        virtualFields.put(fieldName, expression);
    }

    public String getAdaptSelectSql(){
        String fields = String.join(",", getFields());
        String virtualFieldsStr = "";

        if(virtualFields.size() == 0){
            return null;
        }

        for(Map.Entry<String, String> entry : virtualFields.entrySet()){
            virtualFieldsStr += entry.getValue() +" AS " + entry.getKey() + ",";
        }

        if(!Strings.isNullOrEmpty(virtualFieldsStr)){
            fields += "," + virtualFieldsStr.substring(0, virtualFieldsStr.lastIndexOf(","));
        }

        return String.format("select %s from %s", fields, getAdaptName());
    }

    public String getAdaptName(){
        return getName() + "_adapt";
    }
}
