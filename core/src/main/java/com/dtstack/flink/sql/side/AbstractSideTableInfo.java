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

 

package com.dtstack.flink.sql.side;

import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.ClassDataTypeConverter;
import org.apache.flink.table.types.utils.TypeConversions;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Reason:
 * Date: 2018/7/25
 * Company: www.dtstack.com
 * @author xuchao
 */

public abstract class AbstractSideTableInfo extends AbstractTableInfo implements Serializable {

    public static final String TARGET_SUFFIX = "Side";

    public static final String CACHE_KEY = "cache";

    public static final String CACHE_SIZE_KEY = "cacheSize";

    public static final String CACHE_TTLMS_KEY = "cacheTTLMs";

    public static final String PARTITIONED_JOIN_KEY = "partitionedJoin";

    public static final String CACHE_MODE_KEY = "cacheMode";

    public static final String ASYNC_CAP_KEY = "asyncCapacity";

    public static final String ASYNC_TIMEOUT_KEY = "asyncTimeout";

    public static final String ASYNC_FAIL_MAX_NUM_KEY = "asyncFailMaxNum";

    public static final String CONNECT_RETRY_MAX_NUM_KEY = "connectRetryMaxNum";

    public static final String ASYNC_REQ_POOL_KEY = "asyncPoolSize";

    private String cacheType = "none";

    private int cacheSize = 10000;

    private long cacheTimeout = 60 * 1000L;

    private int  asyncCapacity=100;

    private int  asyncTimeout=10000;

    /**
     *  async operator req outside conn pool size, egg rdb conn pool size
     */
    private int asyncPoolSize = 0;

    private boolean partitionedJoin = false;

    private String cacheMode="ordered";

    private Long asyncFailMaxNum;

    private Integer connectRetryMaxNum;

    private List<PredicateInfo>  predicateInfoes = Lists.newArrayList();

    public RowTypeInfo getRowTypeInfo(){
        Class[] fieldClass = getFieldClasses();
        TypeInformation<?>[] types = new TypeInformation[fieldClass.length];
        String[] fieldNames = getFields();
        for(int i=0; i<fieldClass.length; i++){
            types[i] = TypeInformation.of(fieldClass[i]);
        }

        return new RowTypeInfo(types, fieldNames);
    }

    public BaseRowTypeInfo getBaseRowTypeInfo(){
        String[] fieldNames = getFields();
        Class[] fieldClass = getFieldClasses();
        LogicalType[] logicalTypes = new LogicalType[fieldClass.length];
        for (int i = 0; i < fieldClass.length; i++) {
            if(fieldClass[i].getName().equals(BigDecimal.class.getName())){
                logicalTypes[i] = new DecimalType(DecimalType.MAX_PRECISION, 18);
                continue;
            }

            Optional<DataType> optionalDataType = ClassDataTypeConverter.extractDataType(fieldClass[i]);
            if(!optionalDataType.isPresent()){
                throw new RuntimeException(String.format("not support table % field %s type %s", getName(), fieldNames[i], fieldClass[i]));
            }

            logicalTypes[i] = optionalDataType.get().getLogicalType();
        }

        return new BaseRowTypeInfo(logicalTypes, fieldNames);
    }

    public String getCacheType() {
        return cacheType;
    }

    public void setCacheType(String cacheType) {
        this.cacheType = cacheType;
    }

    public int getCacheSize() {
        return cacheSize;
    }

    public void setCacheSize(int cacheSize) {
        this.cacheSize = cacheSize;
    }

    public long getCacheTimeout() {
        return cacheTimeout;
    }

    public void setCacheTimeout(long cacheTimeout) {
        this.cacheTimeout = cacheTimeout;
    }

    public boolean isPartitionedJoin() {
        return partitionedJoin;
    }

    public void setPartitionedJoin(boolean partitionedJoin) {
        this.partitionedJoin = partitionedJoin;
    }

    public String getCacheMode() {
        return cacheMode;
    }

    public void setCacheMode(String cacheMode) {
        this.cacheMode = cacheMode;
    }

    public int getAsyncCapacity() {
        return asyncCapacity;
    }

    public void setAsyncCapacity(int asyncCapacity) {
        this.asyncCapacity = asyncCapacity;
    }

    public int getAsyncTimeout() {
        return asyncTimeout;
    }

    public void setAsyncTimeout(int asyncTimeout) {
        this.asyncTimeout = asyncTimeout;
    }

    public void setPredicateInfoes(List<PredicateInfo> predicateInfoes) {
        this.predicateInfoes = predicateInfoes;
    }

    public List<PredicateInfo> getPredicateInfoes() {
        return predicateInfoes;
    }

    public void addPredicateInfo(PredicateInfo predicateInfo) {
        this.predicateInfoes.add(predicateInfo);
    }

    public Long getAsyncFailMaxNum(Long defaultValue) {
        return Objects.isNull(asyncFailMaxNum) ? defaultValue : asyncFailMaxNum;
    }

    public void setAsyncFailMaxNum(Long asyncFailMaxNum) {
        this.asyncFailMaxNum = asyncFailMaxNum;
    }


    public int getAsyncPoolSize() {
        return asyncPoolSize;
    }

    public void setAsyncPoolSize(int asyncPoolSize) {
        this.asyncPoolSize = asyncPoolSize;
    }


    public Integer getConnectRetryMaxNum(Integer defaultValue) {
        return Objects.isNull(connectRetryMaxNum) ? defaultValue : connectRetryMaxNum;
    }

    public void setConnectRetryMaxNum(Integer connectRetryMaxNum) {
        this.connectRetryMaxNum = connectRetryMaxNum;
    }
    @Override
    public String toString() {
        return "Cache Info{" +
                "cacheType='" + cacheType + '\'' +
                ", cacheSize=" + cacheSize +
                ", cacheTimeout=" + cacheTimeout +
                ", asyncCapacity=" + asyncCapacity +
                ", asyncTimeout=" + asyncTimeout +
                ", asyncPoolSize=" + asyncPoolSize +
                ", asyncFailMaxNum=" + asyncFailMaxNum +
                ", partitionedJoin=" + partitionedJoin +
                ", cacheMode='" + cacheMode + '\'' +
                '}';
    }

}
