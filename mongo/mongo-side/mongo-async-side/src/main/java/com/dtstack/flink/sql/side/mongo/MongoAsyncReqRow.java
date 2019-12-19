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


package com.dtstack.flink.sql.side.mongo;

import com.dtstack.flink.sql.enums.ECacheContentType;
import com.dtstack.flink.sql.side.AsyncReqRow;
import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.SideTableInfo;
import com.dtstack.flink.sql.side.cache.CacheObj;
import com.dtstack.flink.sql.side.mongo.table.MongoSideTableInfo;
import com.dtstack.flink.sql.side.mongo.utils.MongoUtil;
import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoClientSettings;
import com.mongodb.async.client.MongoClients;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ConnectionPoolSettings;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import com.google.common.collect.Lists;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.types.Row;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Reason:
 * Date: 2018/11/6
 *
 * @author xuqianjin
 */
public class MongoAsyncReqRow extends AsyncReqRow {
    private static final long serialVersionUID = -1183158242862673706L;

    private static final Logger LOG = LoggerFactory.getLogger(MongoAsyncReqRow.class);

    private final static int DEFAULT_MAX_DB_CONN_POOL_SIZE = 20;

    private transient MongoClient mongoClient;

    private MongoDatabase db;

    private MongoSideTableInfo MongoSideTableInfo;

    public MongoAsyncReqRow(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) {
        super(new MongoAsyncSideInfo(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo));
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MongoSideTableInfo = (MongoSideTableInfo) sideInfo.getSideTableInfo();
        connMongoDB();
    }

    public void connMongoDB() throws Exception {
        MongoCredential mongoCredential;
        String[] servers = MongoSideTableInfo.getAddress().split(",");
        String host;
        Integer port;
        String[] hostAndPort;
        List<ServerAddress> lists = new ArrayList<>();
        for (String server : servers) {
            hostAndPort = server.split(":");
            host = hostAndPort[0];
            port = Integer.parseInt(hostAndPort[1]);
            lists.add(new ServerAddress(host, port));
        }
        ClusterSettings clusterSettings = ClusterSettings.builder().hosts(lists).build();
        ConnectionPoolSettings connectionPoolSettings = ConnectionPoolSettings.builder()
                .maxSize(DEFAULT_MAX_DB_CONN_POOL_SIZE)
                .build();
        if (!StringUtils.isEmpty(MongoSideTableInfo.getUserName()) || !StringUtils.isEmpty(MongoSideTableInfo.getPassword())) {
            mongoCredential = MongoCredential.createCredential(MongoSideTableInfo.getUserName(), MongoSideTableInfo.getDatabase(),
                    MongoSideTableInfo.getPassword().toCharArray());
            MongoClientSettings settings = MongoClientSettings.builder().credential(mongoCredential)
                    .clusterSettings(clusterSettings)
                    .connectionPoolSettings(connectionPoolSettings)
                    .build();
            mongoClient = MongoClients.create(settings);
        } else {
            MongoClientSettings settings = MongoClientSettings.builder().clusterSettings(clusterSettings)
                    .connectionPoolSettings(connectionPoolSettings)
                    .build();
            mongoClient = MongoClients.create(settings);
        }
        db = mongoClient.getDatabase(MongoSideTableInfo.getDatabase());
    }

    @Override
    public void asyncInvoke(Row input, ResultFuture<Row> resultFuture) throws Exception {

        BasicDBObject basicDBObject = new BasicDBObject();
        for (int i = 0; i < sideInfo.getEqualFieldList().size(); i++) {
            Integer conValIndex = sideInfo.getEqualValIndex().get(i);
            Object equalObj = input.getField(conValIndex);
            if (equalObj == null) {
                dealMissKey(input, resultFuture);
                return;
            }
            basicDBObject.put(sideInfo.getEqualFieldList().get(i), equalObj);
        }
        try {
            // 填充谓词
            sideInfo.getSideTableInfo().getPredicateInfoes().stream().map(info -> {
                BasicDBObject filterCondition = MongoUtil.buildFilterObject(info);
                if (null != filterCondition) {
                    basicDBObject.append(info.getFieldName(), filterCondition);
                }
                return info;
            }).count();
        } catch (Exception e) {
            LOG.info("add predicate infoes error ", e);
        }

        String key = buildCacheKey(basicDBObject.values());
        if (openCache()) {
            CacheObj val = getFromCache(key);
            if (val != null) {

                if (ECacheContentType.MissVal == val.getType()) {
                    dealMissKey(input, resultFuture);
                    return;
                } else if (ECacheContentType.MultiLine == val.getType()) {
                    List<Row> rowList = Lists.newArrayList();
                    for (Object jsonArray : (List) val.getContent()) {
                        Row row = fillData(input, jsonArray);
                        rowList.add(row);
                    }
                    resultFuture.complete(rowList);
                } else {
                    throw new RuntimeException("not support cache obj type " + val.getType());
                }
                return;
            }
        }
        AtomicInteger atomicInteger = new AtomicInteger(0);
        MongoCollection dbCollection = db.getCollection(MongoSideTableInfo.getTableName(), Document.class);
        List<Document> cacheContent = Lists.newArrayList();
        Block<Document> printDocumentBlock = new Block<Document>() {
            @Override
            public void apply(final Document document) {
                atomicInteger.incrementAndGet();
                Row row = fillData(input, document);
                if (openCache()) {
                    cacheContent.add(document);
                }
                resultFuture.complete(Collections.singleton(row));
            }
        };
        SingleResultCallback<Void> callbackWhenFinished = new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                if (atomicInteger.get() <= 0) {
                    LOG.warn("Cannot retrieve the data from the database");
                    resultFuture.complete(null);
                } else {
                    if (openCache()) {
                        putCache(key, CacheObj.buildCacheObj(ECacheContentType.MultiLine, cacheContent));
                    }
                }
            }
        };
        dbCollection.find(basicDBObject).forEach(printDocumentBlock, callbackWhenFinished);
    }

    @Override
    public Row fillData(Row input, Object line) {
        Document doc = (Document) line;
        Row row = new Row(sideInfo.getOutFieldInfoList().size());
        for (Map.Entry<Integer, Integer> entry : sideInfo.getInFieldIndex().entrySet()) {
            Object obj = input.getField(entry.getValue());
            obj = convertTimeIndictorTypeInfo(entry.getValue(), obj);
            row.setField(entry.getKey(), obj);
        }

        for (Map.Entry<Integer, Integer> entry : sideInfo.getSideFieldIndex().entrySet()) {
            if (doc == null) {
                row.setField(entry.getKey(), null);
            } else {
                row.setField(entry.getKey(), doc.get(sideInfo.getSideFieldNameIndex().get(entry.getKey())));
            }
        }

        return row;
    }

    @Override
    public void close() throws Exception {
        super.close();
        try {
            if (mongoClient != null) {
                mongoClient.close();
            }
        } catch (Exception e) {
            throw new RuntimeException("[closeMongoDB]:" + e.getMessage());
        }
    }

    public String buildCacheKey(Collection collection) {
        StringBuilder sb = new StringBuilder();
        for (Object ele : collection) {
            sb.append(ele.toString())
                    .append("_");
        }

        return sb.toString();
    }

}
