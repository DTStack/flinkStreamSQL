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

package com.dtstack.flink.sql.side.kingbase;

import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.rdb.all.AbstractRdbAllReqRow;
import com.dtstack.flink.sql.util.DtStringUtil;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Map;

/**
 * Date: 2020/9/11
 * Company: www.dtstack.com
 *
 * @author tiezhu
 */
public class KingbaseAllReqRow extends AbstractRdbAllReqRow {

    private static final long serialVersionUID = 2021683212163965319L;

    private static final Logger LOG = LoggerFactory.getLogger(KingbaseAllReqRow.class);

    private static final String KINGBASE_DRIVER = "com.kingbase8.Driver";

    public KingbaseAllReqRow(RowTypeInfo rowTypeInfo,
                             JoinInfo joinInfo,
                             List<FieldInfo> outFieldInfoList,
                             AbstractSideTableInfo sideTableInfo) {
        super(new KingbaseAllSideInfo(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo));
    }

    @Override
    public Connection getConn(String dbUrl, String userName, String password) {
        try {
            Class.forName(KINGBASE_DRIVER);
            //add param useCursorFetch=true
            Map<String, String> addParams = Maps.newHashMap();
            addParams.put("useCursorFetch", "true");
            String targetDbUrl = DtStringUtil.addJdbcParam(dbUrl, addParams, true);
            return DriverManager.getConnection(targetDbUrl, userName, password);
        } catch (Exception e) {
            LOG.error("kingbase get connection error", e);
            throw new RuntimeException("kingbase get connect error", e);
        }
    }
}
