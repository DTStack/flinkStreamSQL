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

import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.mongo.MongoAllSideInfo;
import com.dtstack.flink.sql.side.mongo.utils.MongoUtil;
import com.dtstack.flink.sql.side.table.BaseTableFunction;
import com.dtstack.flink.sql.util.SwitchUtil;
import com.google.common.collect.Maps;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @author: chuixue
 * @create: 2020-12-04 09:52
 * @description:Mongo全量维表
 **/
public class MongoTableFunction extends BaseTableFunction {
    private static final long serialVersionUID = -675332795591842778L;

    private static final Logger LOG = LoggerFactory.getLogger(MongoTableFunction.class);
    private MongoClient mongoClient;
    private MongoDatabase db;

    public MongoTableFunction(AbstractSideTableInfo sideTableInfo, String[] lookupKeys) {
        super(new MongoAllSideInfo(sideTableInfo, lookupKeys));
    }

    @Override
    protected void loadData(Object cacheRef) {
        Map<String, List<Map<String, Object>>> tmpCache = (Map<String, List<Map<String, Object>>>) cacheRef;
        MongoSideTableInfo tableInfo = (MongoSideTableInfo) sideInfo.getSideTableInfo();
        MongoCollection dbCollection = null;

        try {
            for (int i = 0; i < CONN_RETRY_NUM; i++) {
                try {
                    dbCollection = getConn(tableInfo.getAddress(), tableInfo.getUserName(), tableInfo.getPassword(),
                            tableInfo.getDatabase(), tableInfo.getTableName());
                    break;
                } catch (Exception e) {
                    if (i == CONN_RETRY_NUM - 1) {
                        throw new RuntimeException("", e);
                    }

                    try {
                        String connInfo = "url:" + tableInfo.getAddress() + ";userName:" + tableInfo.getUserName() + ",pwd:" + tableInfo.getPassword();
                        LOG.warn("get conn fail, wait for 5 sec and try again, connInfo:" + connInfo);
                        Thread.sleep(LOAD_DATA_ERROR_SLEEP_TIME);
                    } catch (InterruptedException e1) {
                        LOG.error("", e1);
                    }
                }
            }

            //load data from table
            String[] sideFieldNames = physicalFields.values().stream().toArray(String[]::new);
            String[] sideFieldTypes = tableInfo.getFieldTypes();
            BasicDBObject basicDBObject = new BasicDBObject();
            for (String selectField : sideFieldNames) {
                basicDBObject.append(selectField, 1);
            }
            BasicDBObject filterObject = new BasicDBObject();
            try {
                // 填充谓词
                sideInfo.getSideTableInfo().getPredicateInfoes().stream().map(info -> {
                    BasicDBObject filterCondition = MongoUtil.buildFilterObject(info);
                    if (null != filterCondition) {
                        filterObject.append(info.getFieldName(), filterCondition);
                    }
                    return info;
                }).count();
            } catch (Exception e) {
                LOG.info("add predicate infoes error ", e);
            }

            FindIterable<Document> findIterable = dbCollection.find(filterObject).projection(basicDBObject).limit(getFetchSize());
            MongoCursor<Document> mongoCursor = findIterable.iterator();
            while (mongoCursor.hasNext()) {
                Document doc = mongoCursor.next();
                Map<String, Object> oneRow = Maps.newHashMap();
                // 防止一条数据有问题，后面数据无法加载
                try {
                    for (int i = 0; i < sideFieldNames.length; i++) {
                        Object object = doc.get(sideFieldNames[i].trim());
                        object = SwitchUtil.getTarget(object, sideFieldTypes[i]);
                        oneRow.put(sideFieldNames[i].trim(), object);
                    }
                    buildCache(oneRow, tmpCache);
                } catch (Exception e) {
                    LOG.error("", e);
                }
            }
        } catch (Exception e) {
            LOG.error("", e);
        } finally {
            if (mongoClient != null) {
                mongoClient.close();
            }
        }
    }

    /**
     * 获取连接
     *
     * @param host
     * @param userName
     * @param password
     * @param database
     * @param tableName
     * @return
     */
    private MongoCollection getConn(String host, String userName, String password, String database, String tableName) {
        MongoCollection dbCollection;
        mongoClient = new MongoClient(new MongoClientURI(getConnectionUrl(host, userName, password)));
        db = mongoClient.getDatabase(database);
        dbCollection = db.getCollection(tableName, Document.class);
        return dbCollection;
    }

    /**
     * 获取连接信息
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
}
