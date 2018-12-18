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

import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.List;

/**
 * Reason:
 * Date: 2018/6/22
 * Company: www.dtstack.com
 * @author xuchao
 */

public abstract class TableInfo implements Serializable {

    public static final String PARALLELISM_KEY = "parallelism";

    public static final String SOURCE_DATA_TYPE = "sourcedatatype";

    public static final String SINK_DATA_TYPE = "sinkdatatype";

    public static final String FIELD_DELINITER = "fielddelimiter";

    public static final String LENGTH_CHECK_POLICY = "lengthcheckpolicy";

    private String name;

    private String type;

    private String[] fields;

    private String[] fieldTypes;

    private Class<?>[] fieldClasses;

    private final List<String> fieldList = Lists.newArrayList();

    private final List<String> fieldTypeList = Lists.newArrayList();

    private final List<Class> fieldClassList = Lists.newArrayList();

    private List<String> primaryKeys;

    private Integer parallelism = 1;

    private String sourceDataType = "json";

    private String sinkDataType = "json";

    private String fieldDelimiter;

    private String lengthCheckPolicy = "SKIP";



    public String[] getFieldTypes() {
        return fieldTypes;
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

    public Class<?>[] getFieldClasses() {
        return fieldClasses;
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
        if(parallelism == null){
            return;
        }

        if(parallelism <= 0){
            throw new RuntimeException("Abnormal parameter settings: parallelism > 0");
        }

        this.parallelism = parallelism;
    }

    public void addField(String fieldName){
        fieldList.add(fieldName);
    }

    public void addFieldClass(Class fieldClass){
        fieldClassList.add(fieldClass);
    }

    public void addFieldType(String fieldType){
        fieldTypeList.add(fieldType);
    }

    public void setFields(String[] fields) {
        this.fields = fields;
    }

    public void setFieldTypes(String[] fieldTypes) {
        this.fieldTypes = fieldTypes;
    }

    public void setFieldClasses(Class<?>[] fieldClasses) {
        this.fieldClasses = fieldClasses;
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

    public String getSourceDataType() {
        return sourceDataType;
    }

    public void setSourceDataType(String sourceDataType) {
        this.sourceDataType = sourceDataType;
    }

    public String getSinkDataType() {
        return sinkDataType;
    }

    public void setSinkDataType(String sinkDataType) {
        this.sinkDataType = sinkDataType;
    }

    public String getFieldDelimiter() {
        return fieldDelimiter;
    }

    public void setFieldDelimiter(String fieldDelimiter) {
        this.fieldDelimiter = fieldDelimiter;
    }

    public String getLengthCheckPolicy() {
        return lengthCheckPolicy;
    }

    public void setLengthCheckPolicy(String lengthCheckPolicy) {
        this.lengthCheckPolicy = lengthCheckPolicy;
    }

    public void finish(){
        this.fields = fieldList.toArray(new String[fieldList.size()]);
        this.fieldClasses = fieldClassList.toArray(new Class[fieldClassList.size()]);
        this.fieldTypes = fieldTypeList.toArray(new String[fieldTypeList.size()]);
    }
}
