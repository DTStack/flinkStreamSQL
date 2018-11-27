/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flink.sql.sink.sqlserver;

import com.dtstack.flink.sql.sink.rdb.RdbSink;

import java.util.List;

/**
 * Reason:
 * Date: 2018/11/27
 * Company: www.dtstack.com
 *
 * @author maqi
 */
public class SqlserverSink extends RdbSink {
    private static final String SQLSERVER_DRIVER = "net.sourceforge.jtds.jdbc.Driver";

    @Override
    public String getDriverName() {
        return SQLSERVER_DRIVER;
    }

    @Override
    public void buildSql(String tableName, List<String> fields) {
        buildInsertSql(tableName, fields);
    }

    private void buildInsertSql(String tableName, List<String> fields) {

    }
}
