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
import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.BaseAsyncReqRow;
import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.cache.CacheObj;
import com.dtstack.flink.sql.side.mongo.table.MongoSideTableInfo;
import com.dtstack.flink.sql.side.mongo.utils.MongoUtil;
import com.dtstack.flink.sql.util.RowDataComplete;
import com.google.common.collect.Lists;
import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoClients;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class MongoAsyncReqRow extends BaseAsyncReqRow {
    private static final long serialVersionUID = -1183158242862673706L;

    private static final Logger LOG = LoggerFactory.getLogger(MongoAsyncReqRow.class);

    private transient MongoClient mongoClient;

    private MongoDatabase db;

    private MongoSideTableInfo mongoSideTableInfo;

    public MongoAsyncReqRow(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, AbstractSideTableInfo sideTableInfo) {
        super(new MongoAsyncSideInfo(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo));
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        mongoSideTableInfo = (MongoSideTableInfo) sideInfo.getSideTableInfo();
        connMongoDb();
    }

    public void connMongoDb() throws Exception {
        ConnectionString connectionString = new ConnectionString(getConnectionUrl(mongoSideTableInfo.getAddress(),
                mongoSideTableInfo.getUserName(), mongoSideTableInfo.getPassword()));
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .build();
        mongoClient = MongoClients.create(settings);
        db = mongoClient.getDatabase(mongoSideTableInfo.getDatabase());
    }

    @Override
    public void handleAsyncInvoke(Map<String, Object> inputParams, BaseRow input, ResultFuture<BaseRow> resultFuture) throws Exception {
        GenericRow genericRow = (GenericRow) input;
        GenericRow inputCopy = GenericRow.copyReference(genericRow);
        BasicDBObject basicDbObject = new BasicDBObject();
        try {
            basicDbObject.putAll(inputParams);
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

        String key = buildCacheKey(inputParams);

        AtomicInteger atomicInteger = new AtomicInteger(0);
        MongoCollection dbCollection = db.getCollection(mongoSideTableInfo.getTableName(), Document.class);
        List<Document> cacheContent = Lists.newArrayList();
        Block<Document> printDocumentBlock = new Block<Document>() {
            @Override
            public void apply(final Document document) {
                atomicInteger.incrementAndGet();
                BaseRow row = fillData(inputCopy, document);
                if (openCache()) {
                    cacheContent.add(document);
                }
                RowDataComplete.completeBaseRow(resultFuture, row);
            }
        };
        SingleResultCallback<Void> callbackWhenFinished = new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                if (atomicInteger.get() <= 0) {
                    LOG.warn("Cannot retrieve the data from the database");
                    resultFuture.complete(Collections.EMPTY_LIST);
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
    public String buildCacheKey(Map<String, Object> inputParams) {
        StringBuilder sb = new StringBuilder();
        for (Object ele : inputParams.values()) {
            sb.append(ele.toString())
                    .append("_");
        }

        return sb.toString();
    }

    @Override
    public BaseRow fillData(BaseRow input, Object line) {
        GenericRow genericRow = (GenericRow) input;
        Document doc = (Document) line;
        GenericRow row = new GenericRow(sideInfo.getOutFieldInfoList().size());
        row.setHeader(genericRow.getHeader());
        for (Map.Entry<Integer, Integer> entry : sideInfo.getInFieldIndex().entrySet()) {
            Object obj = genericRow.getField(entry.getValue());
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

    private String getConnectionUrl(String address, String userName, String password){
        if(address.startsWith("mongodb://") || address.startsWith("mongodb+srv://")){
            return  address;
        }
        if (StringUtils.isNotBlank(userName) && StringUtils.isNotBlank(password)) {
            return String.format("mongodb://%s:%s@%s", userName, password, address);
        }
        return String.format("mongodb://%s", address);
    }

}
