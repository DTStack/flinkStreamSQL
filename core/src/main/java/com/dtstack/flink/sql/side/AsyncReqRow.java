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

import com.dtstack.flink.sql.enums.ECacheType;
import com.dtstack.flink.sql.side.cache.AbsSideCache;
import com.dtstack.flink.sql.side.cache.CacheObj;
import com.dtstack.flink.sql.side.cache.LRUSideCache;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * All interfaces inherit naming rules: type + "AsyncReqRow" such as == "MysqlAsyncReqRow
 * only support Left join / inner join(join),not support right join
 * Date: 2018/7/9
 * Company: www.dtstack.com
 * @author xuchao
 */

public abstract class AsyncReqRow extends RichAsyncFunction<Row, Row> {

    private static final Logger LOG = LoggerFactory.getLogger(AsyncReqRow.class);

    private static final long serialVersionUID = 2098635244857937717L;

    protected RowTypeInfo rowTypeInfo;

    protected List<FieldInfo> outFieldInfoList;

    protected List<String> equalFieldList = Lists.newArrayList();

    protected List<Integer> equalValIndex = Lists.newArrayList();

    protected String sqlCondition = "";

    protected String sideSelectFields = "";

    protected JoinType joinType;

    //key:Returns the value of the position, returns the index values ​​in the input data
    protected Map<Integer, Integer> inFieldIndex = Maps.newHashMap();

    protected Map<Integer, Integer> sideFieldIndex = Maps.newHashMap();

    protected SideTableInfo sideTableInfo;

    protected AbsSideCache sideCache;

    public AsyncReqRow(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList,
                       SideTableInfo sideTableInfo){
        this.rowTypeInfo = rowTypeInfo;
        this.outFieldInfoList = outFieldInfoList;
        this.joinType = joinInfo.getJoinType();
        this.sideTableInfo = sideTableInfo;
        parseSelectFields(joinInfo);
        buildEqualInfo(joinInfo, sideTableInfo);
    }

    private void initCache(){
        if(sideTableInfo.getCacheType() == null || ECacheType.NONE.name().equalsIgnoreCase(sideTableInfo.getCacheType())){
            return;
        }

        if(ECacheType.LRU.name().equalsIgnoreCase(sideTableInfo.getCacheType())){
            sideCache = new LRUSideCache(sideTableInfo);
        }else{
            throw new RuntimeException("not support side cache with type:" + sideTableInfo.getCacheType());
        }

        sideCache.initCache();
    }

    protected CacheObj getFromCache(String key){
        return sideCache.getFromCache(key);
    }

    protected void putCache(String key, CacheObj value){
        sideCache.putCache(key, value);
    }

    protected boolean openCache(){
        return sideCache != null;
    }

    public void parseSelectFields(JoinInfo joinInfo){
        String sideTableName = joinInfo.getSideTableName();
        String nonSideTableName = joinInfo.getNonSideTable();
        List<String> fields = Lists.newArrayList();

        int sideIndex = 0;
        for( int i=0; i<outFieldInfoList.size(); i++){
            FieldInfo fieldInfo = outFieldInfoList.get(i);
            if(fieldInfo.getTable().equalsIgnoreCase(sideTableName)){
                fields.add(fieldInfo.getFieldName());
                sideFieldIndex.put(i, sideIndex);
                sideIndex++;
            }else if(fieldInfo.getTable().equalsIgnoreCase(nonSideTableName)){
                int nonSideIndex = rowTypeInfo.getFieldIndex(fieldInfo.getFieldName());
                inFieldIndex.put(i, nonSideIndex);
            }else{
                throw new RuntimeException("unknown table " + fieldInfo.getTable());
            }
        }

        if(fields.size() == 0){
            throw new RuntimeException("select non field from table " +  sideTableName);
        }

        sideSelectFields = String.join(",", fields);
    }

    public abstract void buildEqualInfo(JoinInfo joinInfo, SideTableInfo sideTableInfo);

    public void dealOneEqualCon(SqlNode sqlNode, String sideTableName){
        if(sqlNode.getKind() != SqlKind.EQUALS){
            throw new RuntimeException("not equal operator.");
        }

        SqlIdentifier left = (SqlIdentifier)((SqlBasicCall)sqlNode).getOperands()[0];
        SqlIdentifier right = (SqlIdentifier)((SqlBasicCall)sqlNode).getOperands()[1];

        String leftTableName = left.getComponent(0).getSimple();
        String leftField = left.getComponent(1).getSimple();

        String rightTableName = right.getComponent(0).getSimple();
        String rightField = right.getComponent(1).getSimple();

        if(leftTableName.equalsIgnoreCase(sideTableName)){
            equalFieldList.add(leftField);
            int equalFieldIndex = -1;
            for(int i=0; i<rowTypeInfo.getFieldNames().length; i++){
                String fieldName = rowTypeInfo.getFieldNames()[i];
                if(fieldName.equalsIgnoreCase(rightField)){
                    equalFieldIndex = i;
                }
            }
            if(equalFieldIndex == -1){
                throw new RuntimeException("can't find equal field " + rightField);
            }

            equalValIndex.add(equalFieldIndex);

        }else if(rightTableName.equalsIgnoreCase(sideTableName)){

            equalFieldList.add(rightField);
            int equalFieldIndex = -1;
            for(int i=0; i<rowTypeInfo.getFieldNames().length; i++){
                String fieldName = rowTypeInfo.getFieldNames()[i];
                if(fieldName.equalsIgnoreCase(leftField)){
                    equalFieldIndex = i;
                }
            }
            if(equalFieldIndex == -1){
                throw new RuntimeException("can't find equal field " + rightField);
            }

            equalValIndex.add(equalFieldIndex);

        }else{
            throw new RuntimeException("resolve equalFieldList error:" + sqlNode.toString());
        }

    }

    protected abstract Row fillData(Row input, Object sideInput);

    protected void dealMissKey(Row input, ResultFuture<Row> resultFuture){
        if(joinType == JoinType.LEFT){
            //Reserved left table data
            Row row = fillData(input, null);
            resultFuture.complete(Collections.singleton(row));
        }else{
            resultFuture.complete(null);
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        initCache();
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
