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

package com.dtstack.flink.sql.side.mongo.table;

import com.dtstack.flink.sql.enums.ECacheContentType;
import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.cache.CacheObj;
import com.dtstack.flink.sql.side.mongo.MongoAsyncSideInfo;
import com.dtstack.flink.sql.side.table.BaseAsyncTableFunction;
import com.dtstack.flink.sql.util.SwitchUtil;
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
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.types.Row;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * @author: chuixue
 * @create: 2020-12-04 10:59
 * @description:Mongo异步维表
 **/
public class MongoAsyncTableFunction extends BaseAsyncTableFunction {
    private static final long serialVersionUID = -1183158242862673706L;

    private static final Logger LOG = LoggerFactory.getLogger(MongoAsyncTableFunction.class);

    private transient MongoClient mongoClient;
    private MongoDatabase db;
    private MongoSideTableInfo mongoSideTableInfo;

    public MongoAsyncTableFunction(AbstractSideTableInfo sideTableInfo, String[] lookupKeys) {
        super(new MongoAsyncSideInfo(sideTableInfo, lookupKeys));
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        mongoSideTableInfo = (MongoSideTableInfo) sideInfo.getSideTableInfo();
        connMongoDb();
    }

    /**
     * 连接数据库
     *
     * @throws Exception
     */
    public void connMongoDb() throws Exception {
        ConnectionString connectionString = new ConnectionString(getConnectionUrl(mongoSideTableInfo.getAddress(),
                mongoSideTableInfo.getUserName(), mongoSideTableInfo.getPassword()));
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .build();
        mongoClient = MongoClients.create(settings);
        db = mongoClient.getDatabase(mongoSideTableInfo.getDatabase());
    }

    /**
     * 连接信息
     *
     * @param address
     * @param userName
     * @param password
     * @return
     */
    private String getConnectionUrl(String address, String userName, String password) {
        if (address.startsWith("mongodb://") || address.startsWith("mongodb+srv://")) {
            return address;
        }
        if (StringUtils.isNotBlank(userName) && StringUtils.isNotBlank(password)) {
            return String.format("mongodb://%s:%s@%s", userName, password, address);
        }
        return String.format("mongodb://%s", address);
    }

    @Override
    public void handleAsyncInvoke(CompletableFuture<Collection<Row>> future, Object... keys) throws Exception {
        String key = buildCacheKey(keys);

        List<String> lookupKeys = Stream
                .of(sideInfo.getLookupKeys())
                .map(e -> physicalFields.getOrDefault(e, e))
                .collect(Collectors.toList());

        String[] lookupKeysArr = lookupKeys.toArray(new String[0]);

        Map<String, Object> inputParams = IntStream
                .range(0, lookupKeys.size())
                .boxed()
                .collect(Collectors.toMap(i -> lookupKeysArr[i], i -> keys[i]));

        BasicDBObject basicDbObject = new BasicDBObject();
        basicDbObject.putAll(inputParams);

        AtomicInteger atomicInteger = new AtomicInteger(0);
        MongoCollection dbCollection = db.getCollection(mongoSideTableInfo.getTableName(), Document.class);
        List<Document> cacheContent = Lists.newArrayList();
        Block<Document> printDocumentBlock = document -> {
            atomicInteger.incrementAndGet();
            Row row = fillData(document);
            if (openCache()) {
                cacheContent.add(document);
            }
            future.complete(Collections.singleton(row));
        };
        SingleResultCallback<Void> callbackWhenFinished = (result, t) -> {
            if (atomicInteger.get() <= 0) {
                LOG.warn("Cannot retrieve the data from the database");
                future.complete(Collections.EMPTY_LIST);
            } else {
                if (openCache()) {
                    putCache(key, CacheObj.buildCacheObj(ECacheContentType.MultiLine, cacheContent));
                }
            }
        };
        dbCollection.find(basicDbObject).forEach(printDocumentBlock, callbackWhenFinished);
    }

    @Override
    protected void fillDataWapper(Object sideInput, String[] sideFieldNames, String[] sideFieldTypes, Row row) {
        Document doc = (Document) sideInput;
        for (int i = 0; i < sideFieldNames.length; i++) {
            row.setField(i, SwitchUtil.getTarget(doc.get(sideFieldNames[i].trim()), sideFieldTypes[i]));
        }
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
}
