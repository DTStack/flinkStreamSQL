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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * parser register udf sql
 * Date: 2018/6/26
 * Company: www.dtstack.com
 * @author xuchao
 */

public class CreateFuncParser implements IParser {

    private static final String FUNC_PATTERN_STR = "(?i)\\s*create\\s+(scala|table|aggregate)\\s+function\\s+(\\S+)\\s+WITH\\s+(\\S+)";

    private static final Pattern FUNC_PATTERN = Pattern.compile(FUNC_PATTERN_STR);

    @Override
    public boolean verify(String sql) {
        return FUNC_PATTERN.matcher(sql).find();
    }

    @Override
    public void parseSql(String sql, SqlTree sqlTree, String planner) {
        Matcher matcher = FUNC_PATTERN.matcher(sql);
        if(matcher.find()){
            String type = matcher.group(1);
            String funcName = matcher.group(2);
            String className = matcher.group(3);
            SqlParserResult result = new SqlParserResult();
            result.setType(type);
            result.setName(funcName);
            result.setClassName(className);
            sqlTree.addFunc(result);
        }
    }


    public static CreateFuncParser newInstance(){
        return new CreateFuncParser();
    }

    public static class SqlParserResult{

        private String name;

        private String className;

        private String type;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getClassName() {
            return className;
        }

        public void setClassName(String className) {
            this.className = className;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }
    }


}
