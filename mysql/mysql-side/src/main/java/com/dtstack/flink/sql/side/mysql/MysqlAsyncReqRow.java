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

 

package com.dtstack.flink.sql.side.mysql;

import com.dtstack.flink.sql.enums.ECacheContentType;
import com.dtstack.flink.sql.side.mysql.table.MysqlSideTableInfo;
import com.dtstack.flink.sql.side.AsyncReqRow;
import com.dtstack.flink.sql.side.CacheMissVal;
import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.SideTableInfo;
import com.dtstack.flink.sql.side.cache.CacheObj;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Mysql dim table
 * Date: 2018/7/27
 * Company: www.dtstack.com
 * @author xuchao
 */

public class MysqlAsyncReqRow extends AsyncReqRow {

    private static final long serialVersionUID = 2098635244857937717L;

    private static final Logger LOG = LoggerFactory.getLogger(MysqlAsyncReqRow.class);

    private transient SQLClient mySQLClient;

    private final static String MYSQL_DRIVER = "com.mysql.jdbc.Driver";

    private final static int DEFAULT_VERTX_EVENT_LOOP_POOL_SIZE = 10;

    private final static int DEFAULT_VERTX_WORKER_POOL_SIZE = 20;

    private final static int DEFAULT_MAX_DB_CONN_POOL_SIZE = 20;


    public MysqlAsyncReqRow(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) {
        super(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);
    }

    @Override
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

    @Override
    public void buildEqualInfo(JoinInfo joinInfo, SideTableInfo sideTableInfo){

        MysqlSideTableInfo mysqlSideTableInfo = (MysqlSideTableInfo) sideTableInfo;

        String sideTableName = joinInfo.getSideTableName();

        SqlNode conditionNode = joinInfo.getCondition();

        List<SqlNode> sqlNodeList = Lists.newArrayList();
        if(conditionNode.getKind() == SqlKind.AND){
            sqlNodeList.addAll(Lists.newArrayList(((SqlBasicCall)conditionNode).getOperands()));
        }else{
            sqlNodeList.add(conditionNode);
        }

        for(SqlNode sqlNode : sqlNodeList){
            dealOneEqualCon(sqlNode, sideTableName);
        }

        sqlCondition = "select ${selectField} from ${tableName} where ";
        for(int i=0; i<equalFieldList.size(); i++){
            String equalField = equalFieldList.get(i);

            sqlCondition += equalField + "=? ";
            if(i != equalFieldList.size() - 1){
                sqlCondition += " and ";
            }
        }

        sqlCondition = sqlCondition.replace("${tableName}", mysqlSideTableInfo.getTableName()).replace("${selectField}", sideSelectFields);
        System.out.println("---------side_exe_sql-----\n" + sqlCondition);
    }

    @Override
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
                throw new RuntimeException("can't deal equal field: " + sqlNode);
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
                throw new RuntimeException("can't deal equal field: " + sqlNode.toString());
            }

            equalValIndex.add(equalFieldIndex);

        }else{
            throw new RuntimeException("resolve equalFieldList error:" + sqlNode.toString());
        }

    }

    //配置暂时走默认配置
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        JsonObject mySQLClientConfig = new JsonObject();
        MysqlSideTableInfo mysqlSideTableInfo = (MysqlSideTableInfo) sideTableInfo;
        mySQLClientConfig.put("url", mysqlSideTableInfo.getUrl())
                .put("driver_class", MYSQL_DRIVER)
                .put("max_pool_size", DEFAULT_MAX_DB_CONN_POOL_SIZE)
                .put("user", mysqlSideTableInfo.getUserName())
                .put("password", mysqlSideTableInfo.getPassword());

        VertxOptions vo = new VertxOptions();
        vo.setEventLoopPoolSize(DEFAULT_VERTX_EVENT_LOOP_POOL_SIZE);
        vo.setWorkerPoolSize(DEFAULT_VERTX_WORKER_POOL_SIZE);
        Vertx vertx = Vertx.vertx(vo);
        //mySQLClient = JDBCClient.createShared(vertx, mySQLClientConfig, "MySQLPool");
        mySQLClient = JDBCClient.createNonShared(vertx, mySQLClientConfig);
    }

    @Override
    public void asyncInvoke(Row input, ResultFuture<Row> resultFuture) throws Exception {

        JsonArray inputParams = new JsonArray();
        for(Integer conValIndex : equalValIndex){
            Object equalObj = input.getField(conValIndex);
            if(equalObj == null){
                resultFuture.complete(null);
            }

            inputParams.add(equalObj);
        }

        String key = buildCacheKey(inputParams);
        if(openCache()){
            CacheObj val = getFromCache(key);
            if(val != null){

                if(ECacheContentType.MissVal == val.getType()){
                    dealMissKey(input, resultFuture);
                    return;
                }else if(ECacheContentType.MultiLine == val.getType()){

                    for(Object jsonArray : (List)val.getContent()){
                        Row row = fillData(input, jsonArray);
                        resultFuture.complete(Collections.singleton(row));
                    }

                }else{
                    throw new RuntimeException("not support cache obj type " + val.getType());
                }
                return;
            }
        }

        mySQLClient.getConnection(conn -> {
            if (conn.failed()) {
                //Treatment failures
                resultFuture.completeExceptionally(conn.cause());
                return;
            }

            final SQLConnection connection = conn.result();
            connection.queryWithParams(sqlCondition, inputParams, rs -> {
                if (rs.failed()) {
                    LOG.error("Cannot retrieve the data from the database");
                    LOG.error("", rs.cause());
                    resultFuture.complete(null);
                    return;
                }

                List<JsonArray> cacheContent = Lists.newArrayList();

                int resultSize = rs.result().getResults().size();
                if(resultSize > 0){
                    for (JsonArray line : rs.result().getResults()) {
                        Row row = fillData(input, line);
                        if(openCache()){
                            cacheContent.add(line);
                        }
                        resultFuture.complete(Collections.singleton(row));
                    }

                    if(openCache()){
                        putCache(key, CacheObj.buildCacheObj(ECacheContentType.MultiLine, cacheContent));
                    }
                }else{
                    dealMissKey(input, resultFuture);
                    if(openCache()){
                        putCache(key, CacheMissVal.getMissKeyObj());
                    }
                }

                // and close the connection
                connection.close(done -> {
                    if (done.failed()) {
                        throw new RuntimeException(done.cause());
                    }
                });
            });
        });
    }

    @Override
    public Row fillData(Row input, Object line){
        JsonArray jsonArray = (JsonArray) line;
        Row row = new Row(outFieldInfoList.size());
        for(Map.Entry<Integer, Integer> entry : inFieldIndex.entrySet()){
            Object obj = input.getField(entry.getValue());
            if(obj instanceof Timestamp){
                obj = ((Timestamp)obj).getTime();
            }
            row.setField(entry.getKey(), obj);
        }

        for(Map.Entry<Integer, Integer> entry : sideFieldIndex.entrySet()){
            if(jsonArray == null){
                row.setField(entry.getKey(), null);
            }else{
                row.setField(entry.getKey(), jsonArray.getValue(entry.getValue()));
            }
        }

        return row;
    }

    @Override
    public void close() throws Exception {
        super.close();
        mySQLClient.close();
    }

    public String buildCacheKey(JsonArray jsonArray){
        StringBuilder sb = new StringBuilder();
        for(Object ele : jsonArray.getList()){
            sb.append(ele.toString())
                    .append("_");
        }

        return sb.toString();
    }

}
