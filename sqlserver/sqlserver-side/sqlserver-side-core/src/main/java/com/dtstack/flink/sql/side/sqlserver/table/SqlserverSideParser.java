/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flink.sql.side.sqlserver.table;

import com.dtstack.flink.sql.core.rdb.JdbcCheckKeys;
import com.dtstack.flink.sql.side.rdb.table.RdbSideParser;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import java.util.Map;

/**
 * Date: 2019/11/26
 * Company: www.dtstack.com
 * @author maqi
 */
public class SqlserverSideParser extends RdbSideParser {
    private static final String CURR_TYPE = "sqlserver";

    @Override
    public AbstractTableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) {
        props.put(JdbcCheckKeys.DRIVER_NAME, "net.sourceforge.jtds.jdbc.Driver");
        AbstractTableInfo sqlServerTableInfo = super.getTableInfo(tableName, fieldsInfo, props);
        sqlServerTableInfo.setType(CURR_TYPE);
        return sqlServerTableInfo;
    }
}
