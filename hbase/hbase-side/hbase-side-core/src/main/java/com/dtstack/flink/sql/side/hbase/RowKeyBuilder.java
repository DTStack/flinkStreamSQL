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

 

package com.dtstack.flink.sql.side.hbase;

import com.dtstack.flink.sql.side.hbase.enums.EReplaceType;
import com.dtstack.flink.sql.util.MD5Utils;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * rowkey rule
 * Date: 2018/8/23
 * Company: www.dtstack.com
 * @author xuchao
 */

public class RowKeyBuilder implements Serializable{

    private static final long serialVersionUID = 2058635242857937717L;

    private static Pattern Md5Operator = Pattern.compile("(?i)^md5\\(\\s*(.*)\\s*\\)$");

    private List<ReplaceInfo> operatorChain = Lists.newArrayList();

    public void init(String rowKeyTempl){
    	operatorChain.addAll(makeFormula(rowKeyTempl));
    }

    /**
     *
     * @param refData
     * @return
     */
    public String getRowKey(Map<String, Object> refData){
    	return buildStr(operatorChain, refData);
    }



    private String buildStr(List<ReplaceInfo> fieldList, Map<String, Object> refData){
        if(CollectionUtils.isEmpty(fieldList)){
            return "";
        }
        StringBuffer sb = new StringBuffer("");
        for(ReplaceInfo replaceInfo : fieldList){

            if(replaceInfo.getType() == EReplaceType.CONSTANT){
                sb.append(replaceInfo.getParam());
                continue;
            }

            if(replaceInfo.getType() == EReplaceType.FUNC){
                sb.append(MD5Utils.getMD5String(buildStr(replaceInfo.getSubReplaceInfos(), refData)));
                continue;
            }
            String replaceName = replaceInfo.getParam();
            if(!refData.containsKey(replaceName)){
                throw new RuntimeException(String.format("build rowKey with field %s which value not found.", replaceName));
            }

            sb.append(refData.get(replaceName));
        }

        return sb.toString();
    }

    public static String[] splitIgnoreQuotaBrackets(String str, String delimiter){
        String splitPatternStr = delimiter + "(?![^()]*+\\))(?![^{}]*+})(?![^\\[\\]]*+\\])";
        return str.split(splitPatternStr);
    }

    /**
     *
     * @param field
     * @return
     */
    public ReplaceInfo getReplaceInfo(String field){

        field = field.trim();
        if(field.length() <= 2){
            throw new RuntimeException(field + " \n" +
                    "Format defined exceptions");
        }

        //判断是不是常量==>''包裹的标识
        if(field.startsWith("'") && field.endsWith("'")){
            ReplaceInfo replaceInfo = new ReplaceInfo(EReplaceType.CONSTANT);
            field = field.substring(1, field.length() - 1);
            replaceInfo.setParam(field);
            return replaceInfo;
        }

        ReplaceInfo replaceInfo = new ReplaceInfo(EReplaceType.PARAM);
        replaceInfo.setParam(field);
        return replaceInfo;
    }

    private List<ReplaceInfo> makeFormula(String formula){
        if(formula == null || formula.length() <= 0){
            return Lists.newArrayList();
        }

        List<ReplaceInfo> result = Lists.newArrayList();
        for(String meta: splitIgnoreQuotaBrackets(formula, "\\+")){
            Matcher matcher = Md5Operator.matcher(meta.trim());
            if(matcher.find()){
                ReplaceInfo replaceInfo = new ReplaceInfo(EReplaceType.FUNC);
                replaceInfo.setSubReplaceInfos(makeFormula(matcher.group(1)));
                result.add(replaceInfo);
            } else {
                result.add(getReplaceInfo(meta));
            }
        }
        return result;
    }
}
