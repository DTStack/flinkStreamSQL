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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Reason:
 * Date: 2018/6/22
 * Company: www.dtstack.com
 *
 * @author xuchao
 */

public abstract class AbstractTableInfo implements Serializable {

    public static final String PARALLELISM_KEY = "parallelism";
    private final List<String> fieldList = Lists.newArrayList();
    private final List<String> fieldTypeList = Lists.newArrayList();
    private final List<Class> fieldClassList = Lists.newArrayList();
    private final List<FieldExtraInfo> fieldExtraInfoList = Lists.newArrayList();
    private String name;
    private String type;
    private String[] fields;
    private String[] fieldTypes;
    private Class<?>[] fieldClasses;
    /**
     * key:别名, value: realField
     */
    private Map<String, String> physicalFields = Maps.newLinkedHashMap();
    private List<String> primaryKeys;

    private Integer parallelism = -1;

    /**
     * 构建脏数据插件的相关信息
     */
    private Properties dirtyProperties;

    public String[] getFieldTypes() {
        return fieldTypes;
    }

    public void setFieldTypes(String[] fieldTypes) {
        this.fieldTypes = fieldTypes;
    }

    public abstract boolean check();

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String[] getFields() {
        return fields;
    }

    public void setFields(String[] fields) {
        this.fields = fields;
    }

    public Class<?>[] getFieldClasses() {
        return fieldClasses;
    }

    public void setFieldClasses(Class<?>[] fieldClasses) {
        this.fieldClasses = fieldClasses;
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public void setPrimaryKeys(List<String> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getParallelism() {
        return parallelism;
    }

    public void setParallelism(Integer parallelism) {
        if (parallelism == null) {
            return;
        }

        if (parallelism <= 0) {
            throw new RuntimeException("Abnormal parameter settings: parallelism > 0");
        }

        this.parallelism = parallelism;
    }

    public void addField(String fieldName) {
        if (fieldList.contains(fieldName)) {
            throw new RuntimeException("redundancy field name " + fieldName + " in table " + getName());
        }

        fieldList.add(fieldName);
    }

    public void addPhysicalMappings(String aliasName, String physicalFieldName) {
        physicalFields.put(aliasName, physicalFieldName);
    }

    public void addFieldClass(Class fieldClass) {
        fieldClassList.add(fieldClass);
    }

    public void addFieldType(String fieldType) {
        fieldTypeList.add(fieldType);
    }

    public void addFieldExtraInfo(FieldExtraInfo extraInfo) {
        fieldExtraInfoList.add(extraInfo);
    }

    public List<String> getFieldList() {
        return fieldList;
    }

    public List<String> getFieldTypeList() {
        return fieldTypeList;
    }

    public List<Class> getFieldClassList() {
        return fieldClassList;
    }

    public Map<String, String> getPhysicalFields() {
        return physicalFields;
    }

    public void setPhysicalFields(Map<String, String> physicalFields) {
        this.physicalFields = physicalFields;
    }

    public List<FieldExtraInfo> getFieldExtraInfoList() {
        return fieldExtraInfoList;
    }

    public Properties getDirtyProperties() {
        dirtyProperties.setProperty("tableName", this.name);
        return dirtyProperties;
    }

    public void setDirtyProperties(Properties dirtyProperties) {
        this.dirtyProperties = dirtyProperties;
    }

    public void finish() {
        this.fields = fieldList.toArray(new String[0]);
        this.fieldClasses = fieldClassList.toArray(new Class[0]);
        this.fieldTypes = fieldTypeList.toArray(new String[0]);
    }

    /**
     * field extra info，used to store `not null` `default 0`...，
     * <p>
     * now, only support not null
     */
    public static class FieldExtraInfo implements Serializable {

        /**
         * default false：allow field is null
         */
        boolean notNull = false;
        /**
         * field length,eg.char(4)
         */
        int length;

        public int getLength() {
            return length;
        }

        public void setLength(int length) {
            this.length = length;
        }

        public boolean getNotNull() {
            return notNull;
        }

        public void setNotNull(boolean notNull) {
            this.notNull = notNull;
        }

        @Override
        public String toString() {
            return "FieldExtraInfo{" +
                    "notNull=" + notNull +
                    ", length=" + length +
                    '}';
        }
    }
}
