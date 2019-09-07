package com.dtstack.flink.sql.util;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.commons.lang3.StringUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @Auther: jiangjunjie
 * @Date: 2019-06-30 14:57
 * @Description:
 */
public class ParseUtils {
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
