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



package com.dtstack.flink.sql.util;

import com.dtstack.flink.sql.enums.ColumnType;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.calcite.shaded.com.google.common.base.Strings;
import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Timestamp;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Reason:
 * Date: 2018/6/22
 * Company: www.dtstack.com
 * @author xuchao
 */

public class DtStringUtil {

    private static final Pattern NO_VERSION_PATTERN = Pattern.compile("([a-zA-Z]+).*");

    private static ObjectMapper objectMapper = new ObjectMapper();


    /**
     * Split the specified string delimiter --- ignored quotes delimiter
     * @param str
     * @param delimiter
     * @return
     */
    public static List<String> splitIgnoreQuota(String str, char delimiter){
        List<String> tokensList = new ArrayList<>();
        boolean inQuotes = false;
        boolean inSingleQuotes = false;
        int bracketLeftNum = 0;
        StringBuilder b = new StringBuilder();
        for (char c : str.toCharArray()) {
            if(c == delimiter){
                if (inQuotes) {
                    b.append(c);
                } else if(inSingleQuotes){
                    b.append(c);
                } else if(bracketLeftNum > 0){
                    b.append(c);
                }else {
                    tokensList.add(b.toString());
                    b = new StringBuilder();
                }
            }else if(c == '\"'){
                inQuotes = !inQuotes;
                b.append(c);
            }else if(c == '\''){
                inSingleQuotes = !inSingleQuotes;
                b.append(c);
            }else if(c == '('){
                bracketLeftNum++;
                b.append(c);
            }else if(c == ')'){
                bracketLeftNum--;
                b.append(c);
            }else{
                b.append(c);
            }
        }

        tokensList.add(b.toString());

        return tokensList;
    }


    public static String replaceIgnoreQuota(String str, String oriStr, String replaceStr){
        String splitPatternStr = oriStr + "(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)(?=(?:[^']*'[^']*')*[^']*$)";
        return str.replaceAll(splitPatternStr, replaceStr);
    }


    public static String col2string(Object column, String type) {
        String rowData = column.toString();
        ColumnType columnType = ColumnType.valueOf(type.toUpperCase());
        Object result = null;
        switch (columnType) {
            case TINYINT:
                result = Byte.valueOf(rowData);
                break;
            case SMALLINT:
                result = Short.valueOf(rowData);
                break;
            case INT:
                result = Integer.valueOf(rowData);
                break;
            case BIGINT:
                result = Long.valueOf(rowData);
                break;
            case FLOAT:
                result = Float.valueOf(rowData);
                break;
            case DOUBLE:
                result = Double.valueOf(rowData);
                break;
            case DECIMAL:
                result = new BigDecimal(rowData);
                break;
            case STRING:
            case VARCHAR:
            case CHAR:
                result = rowData;
                break;
            case BOOLEAN:
                result = Boolean.valueOf(rowData);
                break;
            case DATE:
                result = DateUtil.dateToString((java.util.Date)column);
                break;
            case TIMESTAMP:
                result = DateUtil.timestampToString((java.util.Date)column);
                break;
            default:
                throw new IllegalArgumentException();
        }
        return result.toString();
    }

    public static String getPluginTypeWithoutVersion(String engineType){

        Matcher matcher = NO_VERSION_PATTERN.matcher(engineType);
        if(!matcher.find()){
            return engineType;
        }

        return matcher.group(1);
    }

    /**
     * add specify params to dbUrl
     * @param dbUrl
     * @param addParams
     * @param isForce true:replace exists param
     * @return
     */
    public static String addJdbcParam(String dbUrl, Map<String, String> addParams, boolean isForce){

        if(Strings.isNullOrEmpty(dbUrl)){
            throw new RuntimeException("dburl can't be empty string, please check it.");
        }

        if(addParams == null || addParams.size() == 0){
            return dbUrl;
        }

        String[] splits = dbUrl.split("\\?");
        String preStr = splits[0];
        Map<String, String> params = Maps.newHashMap();
        if(splits.length > 1){
            String existsParamStr = splits[1];
            String[] existsParams = existsParamStr.split("&");
            for(String oneParam : existsParams){
                String[] kv = oneParam.split("=");
                if(kv.length != 2){
                    throw new RuntimeException("illegal dbUrl:" + dbUrl);
                }

                params.put(kv[0], kv[1]);
            }
        }

        for(Map.Entry<String, String> addParam : addParams.entrySet()){
            if(!isForce && params.containsKey(addParam.getKey())){
                continue;
            }

            params.put(addParam.getKey(), addParam.getValue());
        }

        //rebuild dbURL
        StringBuilder sb = new StringBuilder();
        boolean isFirst = true;
        for(Map.Entry<String, String> param : params.entrySet()){
            if(!isFirst){
                sb.append("&");
            }

            sb.append(param.getKey()).append("=").append(param.getValue());
            isFirst = false;
        }

        return preStr + "?" + sb.toString();
    }

    public  static boolean isJosn(String str){
        boolean flag = false;
        if(StringUtils.isNotBlank(str)){
            try {
                objectMapper.readValue(str,Map.class);
                flag = true;
            } catch (Throwable e) {
                flag=false;
            }
        }
        return flag;
    }

    public static Object parse(String str,Class clazz){
        String fieldType = clazz.getName();
        Object object = null;
        if(fieldType.equals(Integer.class.getName())){
            object = Integer.parseInt(str);
        }else if(fieldType.equals(Long.class.getName())){
            object = Long.parseLong(str);
        }else if(fieldType.equals(Byte.class.getName())){
            object = str.getBytes()[0];
        }else if(fieldType.equals(String.class.getName())){
            object = str;
        }else if(fieldType.equals(Float.class.getName())){
            object = Float.parseFloat(str);
        }else if(fieldType.equals(Double.class.getName())){
            object = Double.parseDouble(str);
        }else if (fieldType.equals(Timestamp.class.getName())){
            object = Timestamp.valueOf(str);
        }else{
            throw new RuntimeException("no support field type for sql. the input type:" + fieldType);
        }
        return object;
    }


    public static String firstUpperCase(String str) {
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }

    public static String quoteTable(String table) {
        String[] parts = table.split("\\.");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.length; ++i) {
            if (i != 0) {
                sb.append(".");
            }
            sb.append(DtStringUtil.quoteColumn(parts[i]));
        }
        return sb.toString();
    }

    public static String quoteColumn(String column) {
        return getStartQuote() + column + getEndQuote();
    }

    public static String getStartQuote() {
        return "\"";
    }

    public static String getEndQuote() {
        return "\"";
    }
}
