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


package com.dtstack.flink.sql.sink.mongo;

import com.dtstack.flink.sql.outputformat.DtRichOutputFormat;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.UpdateResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

/**
 * Reason:
 * Date: 2018/11/6
 *
 * @author xuqianjin
 */
public class MongoOutputFormat extends DtRichOutputFormat<Tuple2> {
    private static final Logger LOG = LoggerFactory.getLogger(MongoOutputFormat.class);

    private String address;
    private String tableName;
    private String userName;
    private String password;
    private String database;
    protected String[] fieldNames;
    TypeInformation<?>[] fieldTypes;

    private MongoClient mongoClient;
    private MongoDatabase db;

    private static String PK = "_ID";

    @Override
    public void configure(Configuration parameters) {
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        establishConnection();
        initMetric();
    }

    @Override
    public void writeRecord(Tuple2 tuple2) throws IOException {

        Tuple2<Boolean, Row> tupleTrans = tuple2;
        Boolean retract = tupleTrans.getField(0);
        if (!retract) {
            //FIXME 暂时不处理Mongo删除操作--->Mongo要求有key,所有认为都是可以执行update查找
            return;
        }

        Row record = tupleTrans.getField(1);
        if (record.getArity() != fieldNames.length) {
            return;
        }

        Document doc = new Document();
        MongoCollection dbCollection = db.getCollection(tableName, Document.class);
        for (int i = 0; i < fieldNames.length; i++) {
            doc.append(fieldNames[i], record.getField(i));
        }
        if (doc.containsKey(PK)) {
            Document updateValue = new Document();
            Document filter = new Document(PK.toLowerCase(), new ObjectId(doc.getString(PK)));
            doc.remove(PK);
            updateValue.append("$set", doc);
            UpdateResult updateResult = dbCollection.updateOne(filter, updateValue);
            if (updateResult.getMatchedCount() <= 0) {
                dbCollection.insertOne(doc);
            }
        } else {
            dbCollection.insertOne(doc);
        }

        if (outRecords.getCount() % ROW_PRINT_FREQUENCY == 0){
            LOG.info(record.toString());
        }
        outRecords.inc();
    }

    @Override
    public void close() {
        try {
            if (mongoClient != null) {
                mongoClient.close();
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("[closeMongoDB]:" + e.getMessage());
        }
    }

    private void establishConnection() {
        mongoClient = new MongoClient(new MongoClientURI(address));
        db = mongoClient.getDatabase(database);
    }

    private MongoOutputFormat() {
    }

    public static MongoOutputFormatBuilder buildMongoOutputFormat() {
        return new MongoOutputFormatBuilder();
    }

    public static class MongoOutputFormatBuilder {
        private final MongoOutputFormat format;

        protected MongoOutputFormatBuilder() {
            this.format = new MongoOutputFormat();
        }

        public MongoOutputFormatBuilder setUsername(String username) {
            format.userName = username;
            return this;
        }

        public MongoOutputFormatBuilder setPassword(String password) {
            format.password = password;
            return this;
        }

        public MongoOutputFormatBuilder setAddress(String address) {
            format.address = address;
            return this;
        }

        public MongoOutputFormatBuilder setTableName(String tableName) {
            format.tableName = tableName;
            return this;
        }

        public MongoOutputFormatBuilder setDatabase(String database) {
            format.database = database;
            return this;
        }

        public MongoOutputFormatBuilder setFieldNames(String[] fieldNames) {
            format.fieldNames = fieldNames;
            return this;
        }

        public MongoOutputFormatBuilder setFieldTypes(TypeInformation<?>[] fieldTypes) {
            format.fieldTypes = fieldTypes;
            return this;
        }

        /**
         * Finalizes the configuration and checks validity.
         *
         * @return Configured RetractJDBCOutputFormat
         */
        public MongoOutputFormat finish() {
            if (format.userName == null) {
                LOG.info("Username was not supplied separately.");
            }
            if (format.password == null) {
                LOG.info("Password was not supplied separately.");
            }
            if (format.address == null) {
                throw new IllegalArgumentException("No address URL supplied.");
            }
            if (format.database == null) {
                throw new IllegalArgumentException("No dababase suplied");
            }
            if (format.tableName == null) {
                throw new IllegalArgumentException("No tableName supplied");
            }
            return format;
        }
    }


}
