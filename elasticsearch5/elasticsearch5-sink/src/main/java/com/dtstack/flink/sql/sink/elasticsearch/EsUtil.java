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

package com.dtstack.flink.sql.sink.elasticsearch;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.dtstack.flink.sql.util.DtStringUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utilities for ElasticSearch
 *
 * Company: www.dtstack.com
 * @author huyifan.zju@163.com
 */
public class EsUtil {

    public static Map<String, Object> rowToJsonMap(Row row, List<String> fields, List<String> types) {
        Preconditions.checkArgument(row.getArity() == fields.size());
        Map<String,Object> jsonMap = new HashMap<>();
        int i = 0;
        for(; i < fields.size(); ++i) {
            String field = fields.get(i);
            String[] parts = field.split("\\.");
            Map<String, Object> currMap = jsonMap;
            for(int j = 0; j < parts.length - 1; ++j) {
                String key = parts[j];
                if(currMap.get(key) == null) {
                    currMap.put(key, new HashMap<String,Object>());
                }
                currMap = (Map<String, Object>) currMap.get(key);
            }
            String key = parts[parts.length - 1];
            Object col = row.getField(i);
            if(col != null) {
                Object value = DtStringUtil.col2string(col, types.get(i));
                currMap.put(key, value);
            }

        }

        return jsonMap;
    }

    /**
     * check whether use position to generation doc's id
     * eg : |1,2,3 -> true
     *      |id,name,addr -> false
     * @param ids
     * @return
     */
    public static boolean checkWhetherUsePosition(String ids) {
        boolean flag = true;
        for( String id : StringUtils.split(ids, ",")) {
            if (!NumberUtils.isNumber(id)) {
                flag= false;
                break;
            }
        }
        return flag;
    }


}
