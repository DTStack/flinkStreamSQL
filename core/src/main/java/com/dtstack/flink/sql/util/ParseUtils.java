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

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;

/**
 * @Auther: jiangjunjie
 * @Date: 2019-06-30 14:57
 * @Description:
 */
public class ParseUtils {
    public static void parseSideWhere(SqlNode whereNode, Map<String, String> physicalFields, List<String> whereConditionList) {
        SqlKind sqlKind = whereNode.getKind();
        if ((sqlKind == SqlKind.OR || sqlKind == SqlKind.AND) && ((SqlBasicCall) whereNode).getOperandList().size() == 2) {
            SqlNode[] sqlOperandsList = ((SqlBasicCall) whereNode).getOperands();
            // whereNode是一颗先解析or再解析and的二叉树。二叉树中序遍历，先左子树，其次中间节点，最后右子树
            parseSideWhere(sqlOperandsList[0], physicalFields, whereConditionList);
            whereConditionList.add(sqlKind.name());
            parseSideWhere(sqlOperandsList[1], physicalFields, whereConditionList);
        } else {
            SqlIdentifier sqlIdentifier = (SqlIdentifier) ((SqlBasicCall) whereNode).getOperands()[0];
            String fieldName = null;
            if (sqlIdentifier.names.size() == 1) {
                fieldName = sqlIdentifier.getComponent(0).getSimple();
            } else {
                fieldName = sqlIdentifier.getComponent(1).getSimple();
            }
            if (physicalFields.containsKey(fieldName)) {
                String sideFieldName = physicalFields.get(fieldName);
                // clone SqlIdentifier node
                SqlParserPos sqlParserPos = new SqlParserPos(0, 0);
                SqlIdentifier sqlIdentifierClone = new SqlIdentifier("", null, sqlParserPos);
                List<String> namesClone = Lists.newArrayList();
                for(String name :sqlIdentifier.names){
                    namesClone.add(name);
                }
                sqlIdentifierClone.setNames(namesClone,null);
                // clone SqlBasicCall node
                SqlBasicCall sqlBasicCall = (SqlBasicCall)whereNode;
                SqlNode[] sqlNodes =  sqlBasicCall.getOperands();
                SqlNode[] sqlNodesClone = new SqlNode[sqlNodes.length];
                for (int i = 0; i < sqlNodes.length; i++) {
                    sqlNodesClone[i] = sqlNodes[i];
                }
                SqlBasicCall sqlBasicCallClone = new SqlBasicCall(sqlBasicCall.getOperator(), sqlNodesClone, sqlParserPos);
                // 替换维表中真实字段名
                List<String> names = Lists.newArrayList();
                names.add(sideFieldName);
                sqlIdentifierClone.setNames(names, null);

                sqlBasicCallClone.setOperand(0, sqlIdentifierClone);
                whereConditionList.add(sqlBasicCallClone.toString());
            } else {
                // 如果字段不是维表中字段，删除字段前的链接符
                if (whereConditionList.size() >= 1) {
                    whereConditionList.remove(whereConditionList.size() - 1);
                }
            }
        }
    }

    public static void parseAnd(SqlNode conditionNode, List<SqlNode> sqlNodeList){
        if(conditionNode.getKind() == SqlKind.AND && ((SqlBasicCall)conditionNode).getOperandList().size()==2){
            parseAnd(((SqlBasicCall)conditionNode).getOperands()[0], sqlNodeList);
            sqlNodeList.add(((SqlBasicCall)conditionNode).getOperands()[1]);
        }else{
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
            String operator = parseOperator(joinCondition.getKind());
            sqlJoinCompareOperate.add(operator);
        }
    }

    public static String parseOperator(SqlKind sqlKind) {
        if (StringUtils.equalsIgnoreCase(sqlKind.toString(), "NOT_EQUALS")){
            return "!=";
        }
        return sqlKind.sql;
    }

}