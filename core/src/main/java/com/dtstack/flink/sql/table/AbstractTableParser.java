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

import com.dtstack.flink.sql.util.ClassUtil;
import com.dtstack.flink.sql.util.DtStringUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reason:
 * Date: 2018/7/4
 * Company: www.dtstack.com
 * @author xuchao
 */

public abstract class AbstractTableParser {

    private static final String PRIMARY_KEY = "primaryKey";
    private static final String NEST_JSON_FIELD_KEY = "nestFieldKey";
    private static final String CHAR_TYPE_NO_LENGTH = "CHAR";

    private static Pattern primaryKeyPattern = Pattern.compile("(?i)PRIMARY\\s+KEY\\s*\\((.*)\\)");
    private static Pattern nestJsonFieldKeyPattern = Pattern.compile("(?i)((@*\\S+\\.)*\\S+)\\s+(\\w+)\\s+AS\\s+(\\w+)(\\s+NOT\\s+NULL)?$");
    private static Pattern physicalFieldFunPattern = Pattern.compile("\\w+\\((\\w+)\\)$");
    private static Pattern charTypePattern = Pattern.compile("(?i)CHAR\\((\\d*)\\)$");

    private Map<String, Pattern> patternMap = Maps.newHashMap();

    private Map<String, ITableFieldDealHandler> handlerMap = Maps.newHashMap();

    public AbstractTableParser() {
        addParserHandler(PRIMARY_KEY, primaryKeyPattern, this::dealPrimaryKey);
        addParserHandler(NEST_JSON_FIELD_KEY, nestJsonFieldKeyPattern, this::dealNestField);
    }

    protected boolean fieldNameNeedsUpperCase() {
        return true;
    }

    public abstract AbstractTableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) throws Exception;

    public boolean dealKeyPattern(String fieldRow, AbstractTableInfo tableInfo){
        for(Map.Entry<String, Pattern> keyPattern : patternMap.entrySet()){
            Pattern pattern = keyPattern.getValue();
            String key = keyPattern.getKey();
            Matcher matcher = pattern.matcher(fieldRow);
            if(matcher.find()){
                ITableFieldDealHandler handler = handlerMap.get(key);
                if(handler == null){
                    throw new RuntimeException("parse field [" + fieldRow + "] error.");
                }

                handler.dealPrimaryKey(matcher, tableInfo);
                return true;
            }
        }

        return false;
    }

    public void parseFieldsInfo(String fieldsInfo, AbstractTableInfo tableInfo){

        List<String> fieldRows = DtStringUtil.splitIgnoreQuota(fieldsInfo, ',');
        for(String fieldRow : fieldRows){
            fieldRow = fieldRow.trim();

            if(StringUtils.isBlank(fieldRow)){
                throw new RuntimeException(String.format("table [%s],exists field empty.", tableInfo.getName()));
            }

            String[] fieldInfoArr = fieldRow.split("\\s+");

            String errorMsg = String.format("table [%s] field [%s] format error.", tableInfo.getName(), fieldRow);
            Preconditions.checkState(fieldInfoArr.length >= 2, errorMsg);

            boolean isMatcherKey = dealKeyPattern(fieldRow, tableInfo);
            if (isMatcherKey) {
                continue;
            }

            //Compatible situation may arise in space in the fieldName
            String[] filedNameArr = new String[fieldInfoArr.length - 1];
            System.arraycopy(fieldInfoArr, 0, filedNameArr, 0, fieldInfoArr.length - 1);
            String fieldName = String.join(" ", filedNameArr);
            String fieldType = fieldInfoArr[fieldInfoArr.length - 1 ].trim();

            Class fieldClass = null;
            AbstractTableInfo.FieldExtraInfo fieldExtraInfo = null;

            Matcher matcher = charTypePattern.matcher(fieldType);
            if (matcher.find()) {
                fieldClass = dbTypeConvertToJavaType(CHAR_TYPE_NO_LENGTH);
                fieldExtraInfo = new AbstractTableInfo.FieldExtraInfo();
                fieldExtraInfo.setLength(Integer.valueOf(matcher.group(1)));
            } else {
                fieldClass = dbTypeConvertToJavaType(fieldType);
            }

            tableInfo.addPhysicalMappings(fieldInfoArr[0], fieldInfoArr[0]);
            tableInfo.addField(fieldName);
            tableInfo.addFieldClass(fieldClass);
            tableInfo.addFieldType(fieldType);
            tableInfo.addFieldExtraInfo(fieldExtraInfo);
        }

        tableInfo.finish();
    }

    public void dealPrimaryKey(Matcher matcher, AbstractTableInfo tableInfo){
        String primaryFields = matcher.group(1).trim();
        String[] splitArry = primaryFields.split(",");
        List<String> primaryKes = Lists.newArrayList(splitArry);
        tableInfo.setPrimaryKeys(primaryKes);
    }

    /**
     * add parser for alias field
     * @param matcher
     * @param tableInfo
     */
    protected void dealNestField(Matcher matcher, AbstractTableInfo tableInfo) {
        String physicalField = matcher.group(1);
        Preconditions.checkArgument(!physicalFieldFunPattern.matcher(physicalField).find(),
                "No need to add data types when using functions, The correct way is : strLen(name) as nameSize, ");

        String fieldType = matcher.group(3);
        String mappingField = matcher.group(4);
        Class fieldClass = dbTypeConvertToJavaType(fieldType);
        boolean notNull = matcher.group(5) != null;
        AbstractTableInfo.FieldExtraInfo fieldExtraInfo = new AbstractTableInfo.FieldExtraInfo();
        fieldExtraInfo.setNotNull(notNull);

        tableInfo.addPhysicalMappings(mappingField, physicalField);
        tableInfo.addField(mappingField);
        tableInfo.addFieldClass(fieldClass);
        tableInfo.addFieldType(fieldType);
        tableInfo.addFieldExtraInfo(fieldExtraInfo);
    }

    public Class dbTypeConvertToJavaType(String fieldType) {
        return ClassUtil.stringConvertClass(fieldType);
    }

    protected void addParserHandler(String parserName, Pattern pattern, ITableFieldDealHandler handler) {
        patternMap.put(parserName, pattern);
        handlerMap.put(parserName, handler);
    }
}
