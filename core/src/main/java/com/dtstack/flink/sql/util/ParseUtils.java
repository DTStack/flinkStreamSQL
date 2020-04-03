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

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashBiMap;
import org.apache.calcite.sql.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.*;

import static org.apache.calcite.sql.SqlKind.*;

/**
 * @Auther: jiangjunjie
 * @Date: 2019-06-30 14:57
 * @Description:
 */
public class ParseUtils {
    public static void parseAnd(SqlNode conditionNode, List<SqlNode> sqlNodeList) {
        if (conditionNode.getKind() == SqlKind.AND && ((SqlBasicCall) conditionNode).getOperandList().size() == 2) {
            parseAnd(((SqlBasicCall) conditionNode).getOperands()[0], sqlNodeList);
            sqlNodeList.add(((SqlBasicCall) conditionNode).getOperands()[1]);
        } else {
            sqlNodeList.add(conditionNode);
        }
    }

    public static void parseJoinCompareOperate(SqlNode condition, List<String> sqlJoinCompareOperate) {
        SqlBasicCall joinCondition = (SqlBasicCall) condition;
        if (joinCondition.getKind() == SqlKind.AND) {
            List<SqlNode> operandList = joinCondition.getOperandList();
            for (SqlNode sqlNode : operandList) {
                parseJoinCompareOperate(sqlNode, sqlJoinCompareOperate);
            }
        } else {
            String operator = transformNotEqualsOperator(joinCondition.getKind());
            sqlJoinCompareOperate.add(operator);
        }
    }

    public static String transformNotEqualsOperator(SqlKind sqlKind) {
        if (StringUtils.equalsIgnoreCase(sqlKind.toString(), "NOT_EQUALS")) {
            return "!=";
        }
        return sqlKind.sql;
    }

    /**
     *  parse multi join table name, child query RealTabName is null
     * @param leftJoin
     * @param aliasAndRealTabName
     */
    public static void parseLeftNodeTableName(SqlNode leftJoin, List<Tuple2<String, String>> aliasAndRealTabName, Set<String> sideTableSet) {
        if (leftJoin.getKind() == IDENTIFIER) {
            SqlIdentifier sqlIdentifier = (SqlIdentifier) leftJoin;
            if (sqlIdentifier.names.size() == 1 && !sideTableSet.contains(sqlIdentifier.names.get(0))) {
                aliasAndRealTabName.add(new Tuple2<>(sqlIdentifier.names.get(0), sqlIdentifier.names.get(0)));
            }
        } else if (leftJoin.getKind() == AS) {
            SqlNode sqlNode = ((SqlBasicCall) leftJoin).getOperands()[0];
            if (sideTableSet.contains(sqlNode.toString())) {
                return;
            }
            if (sqlNode.getKind() == IDENTIFIER) {
                aliasAndRealTabName.add(new Tuple2<>(((SqlBasicCall) leftJoin).getOperands()[1].toString(), sqlNode.toString()));
            } else {
                // child query
                aliasAndRealTabName.add(new Tuple2<>(((SqlBasicCall) leftJoin).getOperands()[1].toString(), null));
            }
        } else if (leftJoin.getKind() == JOIN) {
            parseLeftNodeTableName(((SqlJoin) leftJoin).getLeft(), aliasAndRealTabName, sideTableSet);
            parseLeftNodeTableName(((SqlJoin) leftJoin).getRight(), aliasAndRealTabName, sideTableSet);
        }
    }

    public static SqlNode replaceJoinConditionTabName(SqlNode conditionNode, HashBasedTable<String, String, String> mappingTable, String tabAlias) {
        if (conditionNode.getKind() == SqlKind.AND && ((SqlBasicCall) conditionNode).getOperandList().size() == 2) {
            SqlNode[] operands = ((SqlBasicCall) conditionNode).getOperands();
            Arrays.stream(operands).forEach(op -> replaceJoinConditionTabName(op, mappingTable, tabAlias));
        } else {
            SqlNode[] operands = ((SqlBasicCall) conditionNode).getOperands();
            for (int i = 0; i < operands.length; i++) {
                SqlNode sqlNode = operands[i];
                SqlNode replaceNode = replaceNodeInfo(sqlNode, mappingTable, tabAlias);
                operands[i] = replaceNode;
            }
        }
        return conditionNode;
    }

    public static SqlNode replaceNodeInfo(SqlNode parseNode, HashBasedTable<String, String, String> mappingTable, String tabAlias) {
        if (parseNode.getKind() == IDENTIFIER) {
            SqlIdentifier sqlIdentifier = (SqlIdentifier) parseNode;


            return sqlIdentifier;
        }
        return parseNode;
    }

    public static void fillFieldNameMapping(HashBasedTable<String, String, String> midTableMapping, String[] fieldNames, String tableAlias) {
        Arrays.asList(fieldNames).forEach(fieldName -> {
            String mappingTableName = dealDuplicateFieldName(midTableMapping, fieldName);
            midTableMapping.put(tableAlias, fieldName, mappingTableName);
        });
    }

    public static String dealDuplicateFieldName(HashBasedTable<String, String, String> mappingTable, String fieldName) {
        String mappingFieldName = fieldName;
        int index = 1;
        while (!mappingTable.column(mappingFieldName).isEmpty()) {
            mappingFieldName = suffixWithChar(fieldName, '0', index);
            index++;
        }
        return mappingFieldName;
    }

    public static String dealDuplicateFieldName(HashBiMap<String, String> refFieldMap, String fieldName) {
        String mappingFieldName = fieldName;
        int index = 1;
        while (refFieldMap.inverse().get(mappingFieldName) != null ) {
            mappingFieldName = suffixWithChar(fieldName, '0', index);
            index++;
        }

        return mappingFieldName;
    }

    public static String dealDuplicateFieldName(Map<String, String> refFieldMap, String fieldName) {
        String mappingFieldName = fieldName;
        int index = 1;
        while (refFieldMap.containsKey(mappingFieldName)){
            mappingFieldName = suffixWithChar(fieldName, '0', index);
            index++;
        }

        return mappingFieldName;
    }

    public static String suffixWithChar(String str, char padChar, int repeat){
        StringBuilder stringBuilder = new StringBuilder(str);
        for(int i=0; i<repeat; i++){
            stringBuilder.append(padChar);
        }

        return stringBuilder.toString();
    }
}
