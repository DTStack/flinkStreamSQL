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

 

package com.dtstack.flink.sql.table;

import com.dtstack.flink.sql.util.MathUtil;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reason:
 * Date: 2018/7/4
 * Company: www.dtstack.com
 *
 * @author xuchao
 */

public abstract class AbsSourceParser extends AbsTableParser {

    private static final String VIRTUAL_KEY = "virtualFieldKey";

    private static final String WATERMARK_KEY = "waterMarkKey";

    private static Pattern virtualFieldKeyPattern = Pattern.compile("(?i)^(\\S+\\([^\\)]+\\))\\s+AS\\s+(\\w+)$");

    private static Pattern waterMarkKeyPattern = Pattern.compile("(?i)^\\s*WATERMARK\\s+FOR\\s+(\\S+)\\s+AS\\s+withOffset\\(\\s*(\\S+)\\s*,\\s*(\\d+)\\s*\\)$");

    static {
        keyPatternMap.put(VIRTUAL_KEY, virtualFieldKeyPattern);
        keyPatternMap.put(WATERMARK_KEY, waterMarkKeyPattern);

        keyHandlerMap.put(VIRTUAL_KEY, AbsSourceParser::dealVirtualField);
        keyHandlerMap.put(WATERMARK_KEY, AbsSourceParser::dealWaterMark);
    }

    static void dealVirtualField(Matcher matcher, TableInfo tableInfo){
        SourceTableInfo sourceTableInfo = (SourceTableInfo) tableInfo;
        String fieldName = matcher.group(2);
        String expression = matcher.group(1);
        sourceTableInfo.addVirtualField(fieldName, expression);
    }

    static void dealWaterMark(Matcher matcher, TableInfo tableInfo){
        SourceTableInfo sourceTableInfo = (SourceTableInfo) tableInfo;
        String eventTimeField = matcher.group(1);
        //FIXME 暂时不解析第二个row_time_field参数
        Integer offset = MathUtil.getIntegerVal(matcher.group(3));
        sourceTableInfo.setEventTimeField(eventTimeField);
        sourceTableInfo.setMaxOutOrderness(offset);
    }
}
