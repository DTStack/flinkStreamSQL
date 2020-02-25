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
import com.mongodb.ConnectionString;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.MongoClient;
import com.mongodb.MongoClientSettings;
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
import org.apache.flink.table.runtime.types.CRow;
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

    private transient MongoClient mongoClient;

    private MongoDatabase db;

    private MongoSideTableInfo mongoSideTableInfo;

    public MongoAsyncReqRow(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) {
        super(new MongoAsyncSideInfo(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo));
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        mongoSideTableInfo = (MongoSideTableInfo) sideInfo.getSideTableInfo();
        connMongoDb();
    }

    public void connMongoDb() throws Exception {
        String address = mongoSideTableInfo.getAddress();
        ConnectionString connectionString = new ConnectionString(address);

        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .build();
        mongoClient = MongoClients.create(settings);
        db = mongoClient.getDatabase(mongoSideTableInfo.getDatabase());
    }

    @Override
    public void asyncInvoke(CRow input, ResultFuture<CRow> resultFuture) throws Exception {
        CRow inputCopy = new CRow(input.row(), input.change());
        BasicDBObject basicDbObject = new BasicDBObject();
        for (int i = 0; i < sideInfo.getEqualFieldList().size(); i++) {
            Integer conValIndex = sideInfo.getEqualValIndex().get(i);
            Object equalObj = inputCopy.row().getField(conValIndex);
            if (equalObj == null) {
                dealMissKey(inputCopy, resultFuture);
                return;
            }
            basicDbObject.put(sideInfo.getEqualFieldList().get(i), equalObj);
        }
        try {
            // 填充谓词
            sideInfo.getSideTableInfo().getPredicateInfoes().stream().map(info -> {
                BasicDBObject filterCondition = MongoUtil.buildFilterObject(info);
                if (null != filterCondition) {
                    basicDbObject.append(info.getFieldName(), filterCondition);
                }
                return info;
            }).count();
        } catch (Exception e) {
            LOG.info("add predicate infoes error ", e);
        }

        String key = buildCacheKey(basicDbObject.values());
        if (openCache()) {
            CacheObj val = getFromCache(key);
            if (val != null) {

                if (ECacheContentType.MissVal == val.getType()) {
                    dealMissKey(inputCopy, resultFuture);
                    return;
                } else if (ECacheContentType.MultiLine == val.getType()) {
                    List<CRow> rowList = Lists.newArrayList();
                    for (Object jsonArray : (List) val.getContent()) {
                        Row row = fillData(inputCopy.row(), jsonArray);
                        rowList.add(new CRow(row, inputCopy.change()));
                    }
                    resultFuture.complete(rowList);
                } else {
                    throw new RuntimeException("not support cache obj type " + val.getType());
                }
                return;
            }
        }
        AtomicInteger atomicInteger = new AtomicInteger(0);
        MongoCollection dbCollection = db.getCollection(mongoSideTableInfo.getTableName(), Document.class);
        List<Document> cacheContent = Lists.newArrayList();
        Block<Document> printDocumentBlock = new Block<Document>() {
            @Override
            public void apply(final Document document) {
                atomicInteger.incrementAndGet();
                Row row = fillData(inputCopy.row(), document);
                if (openCache()) {
                    cacheContent.add(document);
                }
                resultFuture.complete(Collections.singleton(new CRow(row, inputCopy.change())));
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
        dbCollection.find(basicDbObject).forEach(printDocumentBlock, callbackWhenFinished);
    }

    @Override
    public Row fillData(Row input, Object line) {
        Document doc = (Document) line;
        Row row = new Row(sideInfo.getOutFieldInfoList().size());
        for (Map.Entry<Integer, Integer> entry : sideInfo.getInFieldIndex().entrySet()) {
            Object obj = input.getField(entry.getValue());
            boolean isTimeIndicatorTypeInfo = TimeIndicatorTypeInfo.class.isAssignableFrom(sideInfo.getRowTypeInfo().getTypeAt(entry.getValue()).getClass());

            if (obj instanceof Timestamp && isTimeIndicatorTypeInfo) {
                obj = ((Timestamp) obj).getTime();
            }

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
