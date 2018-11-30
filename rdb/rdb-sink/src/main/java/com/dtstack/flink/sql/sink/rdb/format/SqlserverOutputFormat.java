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
package com.dtstack.flink.sql.sink.rdb.format;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Reason:
 * Date: 2018/11/30
 * Company: www.dtstack.com
 *
 * @author maqi
 */
public class SqlserverOutputFormat extends RetractJDBCOutputFormat {


    @Override
    public boolean isReplaceInsertQuery() throws SQLException {
        fillRealIndexes();
        fillFullColumns();

        if (!getRealIndexes().isEmpty()) {
            for (List<String> value : getRealIndexes().values()) {
                for (String fieldName : getDbSink().getFieldNames()) {
                    if (value.contains(fieldName)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * get db all index
     *
     * @throws SQLException
     */
    public void fillRealIndexes() throws SQLException {
        Map<String, List<String>> map = Maps.newHashMap();
        ResultSet rs = getDbConn().getMetaData().getIndexInfo(null, null, getTableName(), true, false);

        while (rs.next()) {
            String indexName = rs.getString("INDEX_NAME");
            if (!map.containsKey(indexName)) {
                map.put(indexName, new ArrayList<>());
            }
            String column_name = rs.getString("COLUMN_NAME");
            if (StringUtils.isNotBlank(column_name)) {
                column_name = column_name.toUpperCase();
            }
            map.get(indexName).add(column_name);
        }

        for (Map.Entry<String, List<String>> entry : map.entrySet()) {
            String k = entry.getKey();
            List<String> v = entry.getValue();
            if (v != null && v.size() != 0 && v.get(0) != null) {
                getRealIndexes().put(k, v);
            }
        }
    }

    /**
     * get db all column name
     *
     * @throws SQLException
     */
    public void fillFullColumns() throws SQLException {
        String schema = null;
        ResultSet rs = getDbConn().getMetaData().getColumns(null, schema, getTableName(), null);
        while (rs.next()) {
            String columnName = rs.getString("COLUMN_NAME");
            if (StringUtils.isNotBlank(columnName)) {
                getFullField().add(columnName.toUpperCase());
            }
        }
    }


}
