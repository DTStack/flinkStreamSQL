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

 

package com.dtstack.flink.sql.side.hbase.table;

import com.dtstack.flink.sql.table.AbstractSideTableParser;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.util.MathUtil;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.dtstack.flink.sql.table.AbstractTableInfo.PARALLELISM_KEY;

/**
 * hbase field information must include the definition of an alias -> sql which does not allow ":"
 * Date: 2018/8/21
 * Company: www.dtstack.com
 * @author xuchao
 */

public class HbaseSideParser extends AbstractSideTableParser {

    private final static String FIELD_KEY = "fieldKey";

    private final static Pattern FIELD_PATTERN = Pattern.compile("(?i)(.*)\\s+AS\\s+(\\w+)$");

    public static final String HBASE_ZOOKEEPER_QUORUM = "zookeeperQuorum";

    public static final String ZOOKEEPER_PARENT = "zookeeperParent";

    public static final String TABLE_NAME_KEY = "tableName";

    public static final String PRE_ROW_KEY = "preRowKey";

    public static final String CACHE = "cache";

    public HbaseSideParser() {
        addParserHandler(FIELD_KEY, FIELD_PATTERN, this::dealField);
    }

    @Override
    public AbstractTableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) {
        HbaseSideTableInfo hbaseTableInfo = new HbaseSideTableInfo();
        hbaseTableInfo.setName(tableName);
        parseCacheProp(hbaseTableInfo, props);
        parseFieldsInfo(fieldsInfo, hbaseTableInfo);
        hbaseTableInfo.setTableName((String) props.get(TABLE_NAME_KEY.toLowerCase()));
        hbaseTableInfo.setParallelism(MathUtil.getIntegerVal(props.get(PARALLELISM_KEY.toLowerCase())));
        hbaseTableInfo.setHost((String) props.get(HBASE_ZOOKEEPER_QUORUM.toLowerCase()));
        hbaseTableInfo.setParent((String)props.get(ZOOKEEPER_PARENT.toLowerCase()));
        hbaseTableInfo.setPreRowKey(MathUtil.getBoolean(props.get(PRE_ROW_KEY.toLowerCase()), false));
        hbaseTableInfo.setCacheType((String) props.get(CACHE));
        props.entrySet().stream()
                .filter(entity -> entity.getKey().contains("."))
                .map(entity -> hbaseTableInfo.getHbaseConfig().put(entity.getKey(), String.valueOf(entity.getValue())))
                .count();
        return hbaseTableInfo;
    }


    /**
     * hbase 维表的字段定义需要特殊处理
     * @param matcher
     * @param tableInfo
     */
    private void dealField(Matcher matcher, AbstractTableInfo tableInfo){

        HbaseSideTableInfo sideTableInfo = (HbaseSideTableInfo) tableInfo;
        String filedDefineStr = matcher.group(1);
        String aliasStr = matcher.group(2);

        String[] filedInfoArr = filedDefineStr.split("\\s+");
        if(filedInfoArr.length < 2){
            throw new RuntimeException(String.format("table [%s] field [%s] format error.", tableInfo.getName(), matcher.group(0)));
        }

        //兼容可能在fieldName中出现空格的情况
        String[] filedNameArr = new String[filedInfoArr.length - 1];
        System.arraycopy(filedInfoArr, 0, filedNameArr, 0, filedInfoArr.length - 1);
        String fieldName = String.join(" ", filedNameArr);
        String fieldType = filedInfoArr[filedInfoArr.length - 1 ].trim();
        Class fieldClass = dbTypeConvertToJavaType(filedInfoArr[1].trim());

        sideTableInfo.addColumnRealName(fieldName);
        sideTableInfo.addField(aliasStr);
        sideTableInfo.addFieldClass(fieldClass);
        sideTableInfo.addFieldType(fieldType);
        sideTableInfo.putAliasNameRef(aliasStr, fieldName);
        sideTableInfo.addPhysicalMappings(aliasStr, fieldName);
    }


}
