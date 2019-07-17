package com.dtstack.flink.sql.util;

import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

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
}
