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

package com.dtstack.flink.sql.side.oracle;

import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.rdb.async.RdbAsyncSideInfo;
import com.dtstack.flink.sql.side.rdb.table.RdbSideTableInfo;
import com.dtstack.flink.sql.table.TableInfo;
import com.dtstack.flink.sql.util.DtStringUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

import java.util.List;


public class OracleAsyncSideInfo extends RdbAsyncSideInfo {

    public OracleAsyncSideInfo(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, AbstractSideTableInfo sideTableInfo) {
        super(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);
    }

    @Override
    public String getTableName(RdbSideTableInfo rdbSideTableInfo) {
        return DtStringUtil.getTableFullPath(rdbSideTableInfo.getSchema(), rdbSideTableInfo.getTableName());
    }

    @Override
    public  String quoteIdentifier(String identifier) {
        return "\"" + identifier + "\"";
    }

    @Override
    public String wrapperPlaceholder(String fieldName) {
        int pos = sideTableInfo.getFieldList().indexOf(fieldName);
        String type = sideTableInfo.getFieldTypeList().get(pos);

        String sqlDefaultPlaceholder = " ? ";
        String rpadFormat = "rpad(?, %d, ' ')";

        if (StringUtils.contains(type.toLowerCase(), "char")) {
            TableInfo.FieldExtraInfo fieldExtraInfo = sideTableInfo.getFieldExtraInfoList().get(pos);
            int charLength = fieldExtraInfo == null ? 0 : fieldExtraInfo.getLength();
            if (charLength > 0) {
                return String.format(rpadFormat, charLength);
            }
        }
        return sqlDefaultPlaceholder;
    }
}
