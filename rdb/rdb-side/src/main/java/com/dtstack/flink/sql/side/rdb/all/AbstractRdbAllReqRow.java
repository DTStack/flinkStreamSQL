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

package com.dtstack.flink.sql.side.rdb.all;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;


import com.dtstack.flink.sql.side.BaseAllReqRow;
import com.dtstack.flink.sql.side.BaseSideInfo;
import com.dtstack.flink.sql.side.rdb.table.RdbSideTableInfo;
import com.dtstack.flink.sql.side.rdb.util.SwitchUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.sql.JoinType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Calendar;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * side operator with cache for all(period reload)
 * Date: 2018/11/26
 * Company: www.dtstack.com
 *
 * @author maqi
 */

public abstract class AbstractRdbAllReqRow extends BaseAllReqRow {

    private static final long serialVersionUID = 2098635140857937718L;

    private static final Logger LOG = LoggerFactory.getLogger(AbstractRdbAllReqRow.class);

    private static final int CONN_RETRY_NUM = 3;

    private static final int DEFAULT_FETCH_SIZE = 1000;

    private AtomicReference<Map<String, List<Map<String, Object>>>> cacheRef = new AtomicReference<>();

    public AbstractRdbAllReqRow(BaseSideInfo sideInfo) {
        super(sideInfo);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        RdbSideTableInfo tableInfo = (RdbSideTableInfo) sideInfo.getSideTableInfo();
        LOG.info("rdb dim table config info: {} ", tableInfo.toString());
    }

    @Override
    protected void initCache() throws SQLException {
        Map<String, List<Map<String, Object>>> newCache = Maps.newConcurrentMap();
        cacheRef.set(newCache);
        loadData(newCache);
    }

    @Override
    protected void reloadCache() {
        //reload cacheRef and replace to old cacheRef
        Map<String, List<Map<String, Object>>> newCache = Maps.newConcurrentMap();
        try {
            loadData(newCache);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        cacheRef.set(newCache);
        LOG.info("----- rdb all cacheRef reload end:{}", Calendar.getInstance());
    }

    @Override
    public void flatMap(CRow value, Collector<CRow> out) throws Exception {
        List<Integer> equalValIndex = sideInfo.getEqualValIndex();
        ArrayList<Object> inputParams = equalValIndex.stream()
                .map(value.row()::getField)
                .filter(Objects::nonNull)
                .collect(Collectors.toCollection(ArrayList::new));

        if (inputParams.size() != equalValIndex.size() && sideInfo.getJoinType() == JoinType.LEFT) {
            out.collect(new CRow(fillData(value.row(), null), value.change()));
            return;
        }

        String cacheKey = inputParams.stream()
                .map(Object::toString)
                .collect(Collectors.joining("_"));

        List<Map<String, Object>> cacheList = cacheRef.get().get(cacheKey);
        if (CollectionUtils.isEmpty(cacheList) && sideInfo.getJoinType() == JoinType.LEFT) {
            out.collect(new CRow(fillData(value.row(), null), value.change()));
            return;
        }

        if (CollectionUtils.isEmpty(cacheList)) {
            return;
        }

        cacheList.forEach(one -> out.collect(new CRow(fillData(value.row(), one), value.change())));
    }

    @Override
    public Row fillData(Row input, Object sideInput) {
        Map<String, Object> cacheInfo = (Map<String, Object>) sideInput;
        Row row = new Row(sideInfo.getOutFieldInfoList().size());

        for (Map.Entry<Integer, Integer> entry : sideInfo.getInFieldIndex().entrySet()) {
            // origin value
            Object obj = input.getField(entry.getValue());
            obj = dealTimeAttributeType(sideInfo.getRowTypeInfo().getTypeAt(entry.getValue()).getClass(), obj);
            row.setField(entry.getKey(), obj);
        }

        for (Map.Entry<Integer, String> entry : sideInfo.getSideFieldNameIndex().entrySet()) {
            if (cacheInfo == null) {
                row.setField(entry.getKey(), null);
            } else {
                row.setField(entry.getKey(), cacheInfo.get(entry.getValue()));
            }

        }
        return row;
    }

    /**
     * covert flink time attribute.Type information for indicating event or processing time.
     * However, it behaves like a regular SQL timestamp but is serialized as Long.
     *
     * @param entry
     * @param obj
     * @return
     */
    protected Object dealTimeAttributeType(Class<? extends TypeInformation> entry, Object obj) {
        boolean isTimeIndicatorTypeInfo = TimeIndicatorTypeInfo.class.isAssignableFrom(entry);
        if (obj instanceof Timestamp && isTimeIndicatorTypeInfo) {
            obj = ((Timestamp) obj).getTime();
        }
        return obj;
    }

    private void loadData(Map<String, List<Map<String, Object>>> tmpCache) throws SQLException {
        RdbSideTableInfo tableInfo = (RdbSideTableInfo) sideInfo.getSideTableInfo();
        Connection connection = null;

        try {
            for (int i = 0; i < CONN_RETRY_NUM; i++) {
                try {
                    connection = getConn(tableInfo.getUrl(), tableInfo.getUserName(), tableInfo.getPassword());
                    break;
                } catch (Exception e) {
                    if (i == CONN_RETRY_NUM - 1) {
                        throw new RuntimeException("", e);
                    }
                    try {
                        String connInfo = "url:" + tableInfo.getUrl() + ";userName:" + tableInfo.getUserName() + ",pwd:" + tableInfo.getPassword();
                        LOG.warn("get conn fail, wait for 5 sec and try again, connInfo:" + connInfo);
                        Thread.sleep(5 * 1000);
                    } catch (InterruptedException e1) {
                        LOG.error("", e1);
                    }
                }
            }
            queryAndFillData(tmpCache, connection);
        } catch (Exception e) {
            LOG.error("", e);
            throw new SQLException(e);
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    private void queryAndFillData(Map<String, List<Map<String, Object>>> tmpCache, Connection connection) throws SQLException {
        //load data from table
        String sql = sideInfo.getSqlCondition();
        Statement statement = connection.createStatement();
        statement.setFetchSize(getFetchSize());
        ResultSet resultSet = statement.executeQuery(sql);

        String[] sideFieldNames = StringUtils.split(sideInfo.getSideSelectFields(), ",");
        String[] fields = sideInfo.getSideTableInfo().getFieldTypes();
        while (resultSet.next()) {
            Map<String, Object> oneRow = Maps.newHashMap();
            for (String fieldName : sideFieldNames) {
                Object object = resultSet.getObject(fieldName.trim());
                int fieldIndex = sideInfo.getSideTableInfo().getFieldList().indexOf(fieldName.trim());
                object = SwitchUtil.getTarget(object, fields[fieldIndex]);
                oneRow.put(fieldName.trim(), object);
            }

            String cacheKey = sideInfo.getEqualFieldList().stream()
                    .map(oneRow::get)
                    .map(Object::toString)
                    .collect(Collectors.joining("_"));

            tmpCache.computeIfAbsent(cacheKey, key -> Lists.newArrayList())
                    .add(oneRow);
        }
    }

    public int getFetchSize() {
        return DEFAULT_FETCH_SIZE;
    }

    /**
     * get jdbc connection
     *
     * @param dbURL
     * @param userName
     * @param password
     * @return
     */
    public abstract Connection getConn(String dbURL, String userName, String password);

}
