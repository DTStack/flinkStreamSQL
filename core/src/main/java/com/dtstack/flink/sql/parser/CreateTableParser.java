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

 

package com.dtstack.flink.sql.parser;

import com.dtstack.flink.sql.util.DtStringUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 解析创建表结构sql
 * Date: 2018/6/26
 * Company: www.dtstack.com
 * @author xuchao
 */

public class CreateTableParser implements IParser {

    private static final String PATTERN_STR = "(?i)create\\s+table\\s+(\\S+)\\s*\\((.+)\\)\\s*with\\s*\\((.+)\\)";

    private static final Pattern PATTERN = Pattern.compile(PATTERN_STR);

    public static CreateTableParser newInstance(){
        return new CreateTableParser();
    }

    @Override
    public boolean verify(String sql) {
        return PATTERN.matcher(sql).find();
    }

    @Override
    public void parseSql(String sql, SqlTree sqlTree) {
        Matcher matcher = PATTERN.matcher(sql);
        if(matcher.find()){
            String tableName = matcher.group(1).toUpperCase();
            String fieldsInfoStr = matcher.group(2);
            String propsStr = matcher.group(3);
            Map<String, Object> props = parseProp(propsStr);

            SqlParserResult result = new SqlParserResult();
            result.setTableName(tableName);
            result.setFieldsInfoStr(fieldsInfoStr);
            result.setPropMap(props);

            sqlTree.addPreDealTableInfo(tableName, result);
        }
    }

    private Map parseProp(String propsStr){
        List<String> list = DtStringUtil.splitIgnoreQuota(propsStr.trim(), ',');
        Map<String, Object> propMap = Maps.newHashMap();
        if (CollectionUtils.isNotEmpty(list)){
            for(int i=0; i<list.size(); i++){
                List<String> ss = DtStringUtil.splitIgnoreQuota(list.get(i), '=');
                String key = ss.get(0).trim();
                String value = ss.get(1).trim().replaceAll("'", "").trim();
                propMap.put(key, value);
            }
        }
        return propMap;
    }

    public static class SqlParserResult{

        private String tableName;

        private String fieldsInfoStr;

        private Map<String, Object> propMap;

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public String getFieldsInfoStr() {
            return fieldsInfoStr;
        }

        public void setFieldsInfoStr(String fieldsInfoStr) {
            this.fieldsInfoStr = fieldsInfoStr;
        }

        public Map<String, Object> getPropMap() {
            return propMap;
        }

        public void setPropMap(Map<String, Object> propMap) {
            this.propMap = propMap;
        }
    }
}
