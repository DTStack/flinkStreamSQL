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

 

package com.dtstack.flink.sql.sink.wtable.table;


import com.dtstack.flink.sql.table.AbsTableParser;
import com.dtstack.flink.sql.table.TableInfo;
import com.dtstack.flink.sql.util.MathUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import java.util.Map;

import static com.dtstack.flink.sql.table.TableInfo.PARALLELISM_KEY;

public class WtableSinkParser extends AbsTableParser {


    public static final String NAME_CENTER = "nameCenter";

    public static final String BID = "bid";

    public static final String PASSWORD = "password";

    public static final String WTABLE_ROWKEY = "rowkey";

    public static final String TABLE_ID_KEY = "tableid";

    public static final String CACHE_TTL_MS = "cachettlms";

    @Override
    protected boolean fieldNameNeedsUpperCase() {
        return false;
    }

    @Override
    public TableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) {
        WtableTableInfo wtableTableInfo = new WtableTableInfo();
        wtableTableInfo.setName(tableName);
        parseFieldsInfo(fieldsInfo, wtableTableInfo);
        wtableTableInfo.setNameCenter((String) props.get(NAME_CENTER.toLowerCase()));
        wtableTableInfo.setBid((String)props.get(BID.toLowerCase()));
        wtableTableInfo.setPassword((String)props.get(PASSWORD.toLowerCase()));
        wtableTableInfo.setTableId(MathUtil.getIntegerVal(props.get(TABLE_ID_KEY.toLowerCase())));
        wtableTableInfo.setParallelism(MathUtil.getIntegerVal(props.get(PARALLELISM_KEY.toLowerCase())));
        wtableTableInfo.setCachettlms(MathUtil.getIntegerVal(props.get(CACHE_TTL_MS.toLowerCase())));

        String rk = (String) props.get(WTABLE_ROWKEY.toLowerCase());
        if(StringUtils.isNotBlank(rk)) {
            wtableTableInfo.setRowkey(rk.split(","));
        }
        if(CollectionUtils.isEmpty(wtableTableInfo.getPrimaryKeys()) && ArrayUtils.isEmpty(wtableTableInfo.getRowkey())){
            throw new IllegalStateException("Primary key must be set up.");
        }
        return wtableTableInfo;
    }
}
