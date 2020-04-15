package com.dtstack.flink.sql.util;

import com.dtstack.flink.sql.side.FieldReplaceInfo;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.collections.CollectionUtils;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.apache.calcite.sql.SqlKind.*;

/**
 * 替换 字段
 */
public class FieldReplaceUtil {

    /**
     * 需要考虑更多的情况
     */
    public static void replaceFieldName(SqlNode sqlNode,
                                        String oldTbName,
                                        String newTbName,
                                        Map<String, String> mappingField) {
        SqlKind sqlKind = sqlNode.getKind();
        switch (sqlKind) {
            case INSERT:
                SqlNode sqlSource = ((SqlInsert) sqlNode).getSource();
                replaceFieldName(sqlSource, oldTbName, newTbName, mappingField);
                break;
            case AS:
                SqlNode asNode = ((SqlBasicCall) sqlNode).getOperands()[0];
                replaceFieldName(asNode, oldTbName, newTbName, mappingField);
                break;
            case SELECT:
                SqlSelect sqlSelect = (SqlSelect) sqlNode;
                SqlNodeList sqlSelectList = sqlSelect.getSelectList();
                SqlNode whereNode = sqlSelect.getWhere();
                SqlNodeList sqlGroup = sqlSelect.getGroup();

                //TODO 抽取,暂时使用使用单个join条件作为测试
                if(sqlSelect.getFrom().getKind().equals(JOIN)){
                    SqlJoin joinNode = (SqlJoin) sqlSelect.getFrom();
                    SqlNode joinCondition = joinNode.getCondition();
                    replaceFieldName(((SqlBasicCall)joinCondition).operands[0], oldTbName, newTbName, mappingField);
                    replaceFieldName(((SqlBasicCall)joinCondition).operands[1], oldTbName, newTbName, mappingField);
                }

                //TODO 暂时不处理having
                SqlNode sqlHaving = sqlSelect.getHaving();

                List<SqlNode> newSelectNodeList = Lists.newArrayList();
                for( int i=0; i<sqlSelectList.getList().size(); i++){
                    SqlNode selectNode = sqlSelectList.getList().get(i);
                    //特殊处理 isStar的标识
                    if(selectNode.getKind() == IDENTIFIER && ((SqlIdentifier) selectNode).isStar()){
                        //List<SqlNode> replaceNodeList = replaceSelectStarFieldName(selectNode, replaceInfo);
                        //newSelectNodeList.addAll(replaceNodeList);
                        throw new RuntimeException("not support table.* now");
                    }

                    SqlNode replaceNode = replaceSelectFieldName(selectNode, oldTbName, newTbName, mappingField);
                    if(replaceNode == null){
                        continue;
                    }

                    newSelectNodeList.add(replaceNode);
                }

                SqlNodeList newSelectList = new SqlNodeList(newSelectNodeList, sqlSelectList.getParserPosition());
                sqlSelect.setSelectList(newSelectList);

                //where
                if(whereNode != null){
                    SqlNode[] sqlNodeList = ((SqlBasicCall)whereNode).getOperands();
                    for(int i =0; i<sqlNodeList.length; i++) {
                        SqlNode whereSqlNode = sqlNodeList[i];
                        SqlNode replaceNode = replaceNodeInfo(whereSqlNode, oldTbName, newTbName, mappingField);
                        sqlNodeList[i] = replaceNode;
                    }
                }
                if(sqlGroup != null && CollectionUtils.isNotEmpty(sqlGroup.getList())){
                    for( int i=0; i<sqlGroup.getList().size(); i++){
                        SqlNode selectNode = sqlGroup.getList().get(i);
                        SqlNode replaceNode = replaceNodeInfo(selectNode, oldTbName, newTbName, mappingField);
                        sqlGroup.set(i, replaceNode);
                    }
                }

                break;
            case UNION:
                SqlNode unionLeft = ((SqlBasicCall) sqlNode).getOperands()[0];
                SqlNode unionRight = ((SqlBasicCall) sqlNode).getOperands()[1];
                replaceFieldName(unionLeft, oldTbName, newTbName, mappingField);
                replaceFieldName(unionRight, oldTbName, newTbName, mappingField);

                break;
            case ORDER_BY:
                SqlOrderBy sqlOrderBy  = (SqlOrderBy) sqlNode;
                replaceFieldName(sqlOrderBy.query, oldTbName, newTbName, mappingField);
                SqlNodeList orderFiledList = sqlOrderBy.orderList;
                for (int i=0 ;i<orderFiledList.size(); i++) {
                    SqlNode replaceNode = replaceOrderByTableName(orderFiledList.get(i), oldTbName, newTbName, mappingField);
                    orderFiledList.set(i, replaceNode);
                }

            default:
                break;
        }
    }


    private static SqlNode replaceOrderByTableName(SqlNode orderNode,
                                                   String oldTbName,
                                                   String newTbName,
                                                   Map<String, String> mappingField) {
        if(orderNode.getKind() == IDENTIFIER){
            return createNewIdentify((SqlIdentifier) orderNode, oldTbName, newTbName, mappingField);
        } else if (orderNode instanceof  SqlBasicCall) {
            SqlBasicCall sqlBasicCall = (SqlBasicCall) orderNode;
            for(int i=0; i<sqlBasicCall.getOperandList().size(); i++){
                SqlNode sqlNode = sqlBasicCall.getOperandList().get(i);
                sqlBasicCall.getOperands()[i] = replaceOrderByTableName(sqlNode , oldTbName, newTbName, mappingField);
            }
            return sqlBasicCall;
        } else {
            return orderNode;
        }
    }

    private static SqlNode replaceNodeInfo(SqlNode groupNode,
                                           String oldTbName,
                                           String newTbName,
                                           Map<String, String> mappingField){
        if(groupNode.getKind() == IDENTIFIER){
            return createNewIdentify((SqlIdentifier) groupNode, oldTbName, newTbName, mappingField);
        }else if(groupNode instanceof  SqlBasicCall){
            SqlBasicCall sqlBasicCall = (SqlBasicCall) groupNode;
            for(int i=0; i<sqlBasicCall.getOperandList().size(); i++){
                SqlNode sqlNode = sqlBasicCall.getOperandList().get(i);
                SqlNode replaceNode = replaceSelectFieldName(sqlNode, oldTbName, newTbName, mappingField);
                sqlBasicCall.getOperands()[i] = replaceNode;
            }

            return sqlBasicCall;
        } else if (groupNode.getKind() == CASE) {
            SqlCase sqlCase = (SqlCase) groupNode;

            for (int i = 0; i < sqlCase.getWhenOperands().size(); i++) {
                SqlNode sqlNode = sqlCase.getWhenOperands().getList().get(i);
                SqlNode replaceNode = replaceSelectFieldName(sqlNode, oldTbName, newTbName, mappingField);
                sqlCase.getWhenOperands().set(i,replaceNode);
            }

            for (int i = 0; i < sqlCase.getThenOperands().size(); i++) {
                SqlNode sqlNode = sqlCase.getThenOperands().getList().get(i);
                SqlNode replaceNode = replaceSelectFieldName(sqlNode, oldTbName, newTbName, mappingField);
                sqlCase.getThenOperands().set(i,replaceNode);
            }
            return sqlCase;
        } else {
            return groupNode;
        }
    }

    public static SqlIdentifier createNewIdentify(SqlIdentifier sqlIdentifier,
                                    String oldTbName,
                                    String newTbName,
                                    Map<String, String> mappingField){

        if (sqlIdentifier.names.size() == 1) {
            return sqlIdentifier;
        }

        String tableName = sqlIdentifier.names.get(0);
        String fieldName = sqlIdentifier.names.get(1);
        if(!tableName.equalsIgnoreCase(oldTbName)){
            return sqlIdentifier;
        }

        String mappingFieldName = mappingField.get(fieldName);
        if(mappingFieldName == null){
            return sqlIdentifier;
        }

        sqlIdentifier = sqlIdentifier.setName(0, newTbName);
        sqlIdentifier = sqlIdentifier.setName(1, mappingFieldName);
        return sqlIdentifier;
    }

    public static boolean filterNodeWithTargetName(SqlNode sqlNode, String targetTableName) {

        SqlKind sqlKind = sqlNode.getKind();
        switch (sqlKind){
            case SELECT:
                SqlNode fromNode = ((SqlSelect)sqlNode).getFrom();
                if(fromNode.getKind() == AS && ((SqlBasicCall)fromNode).getOperands()[0].getKind() == IDENTIFIER){
                    if(((SqlBasicCall)fromNode).getOperands()[0].toString().equalsIgnoreCase(targetTableName)){
                        return true;
                    }else{
                        return false;
                    }
                }else{
                    return filterNodeWithTargetName(fromNode, targetTableName);
                }
            case AS:
                SqlNode aliasName = ((SqlBasicCall)sqlNode).getOperands()[1];
                return aliasName.toString().equalsIgnoreCase(targetTableName);
            case JOIN:
                SqlNode leftNode = ((SqlJoin)sqlNode).getLeft();
                SqlNode rightNode =  ((SqlJoin)sqlNode).getRight();
                boolean leftReturn = filterNodeWithTargetName(leftNode, targetTableName);
                boolean rightReturn = filterNodeWithTargetName(rightNode, targetTableName);

                return leftReturn || rightReturn;

            default:
                return false;
        }
    }

    public static SqlNode replaceSelectFieldName(SqlNode selectNode,
                                                 String oldTbName,
                                                 String newTbName,
                                                 Map<String, String> mappingField) {

        if (selectNode.getKind() == AS) {
            SqlNode leftNode = ((SqlBasicCall) selectNode).getOperands()[0];
            SqlNode replaceNode = replaceSelectFieldName(leftNode, oldTbName, newTbName, mappingField);
            if (replaceNode != null) {
                ((SqlBasicCall) selectNode).getOperands()[0] = replaceNode;
            }

            return selectNode;
        }else if(selectNode.getKind() == IDENTIFIER){
            return createNewIdentify((SqlIdentifier) selectNode, oldTbName, newTbName, mappingField);
        }else if(selectNode.getKind() == LITERAL || selectNode.getKind() == LITERAL_CHAIN){//字面含义
            return selectNode;
        }else if(  AGGREGATE.contains(selectNode.getKind())
                || AVG_AGG_FUNCTIONS.contains(selectNode.getKind())
                || COMPARISON.contains(selectNode.getKind())
                || selectNode.getKind() == OTHER_FUNCTION
                || selectNode.getKind() == DIVIDE
                || selectNode.getKind() == CAST
                || selectNode.getKind() == TRIM
                || selectNode.getKind() == TIMES
                || selectNode.getKind() == PLUS
                || selectNode.getKind() == NOT_IN
                || selectNode.getKind() == OR
                || selectNode.getKind() == AND
                || selectNode.getKind() == MINUS
                || selectNode.getKind() == TUMBLE
                || selectNode.getKind() == TUMBLE_START
                || selectNode.getKind() == TUMBLE_END
                || selectNode.getKind() == SESSION
                || selectNode.getKind() == SESSION_START
                || selectNode.getKind() == SESSION_END
                || selectNode.getKind() == HOP
                || selectNode.getKind() == HOP_START
                || selectNode.getKind() == HOP_END
                || selectNode.getKind() == BETWEEN
                || selectNode.getKind() == IS_NULL
                || selectNode.getKind() == IS_NOT_NULL
                || selectNode.getKind() == CONTAINS
                || selectNode.getKind() == TIMESTAMP_ADD
                || selectNode.getKind() == TIMESTAMP_DIFF
                || selectNode.getKind() == LIKE
                || selectNode.getKind() == COALESCE

        ){
            SqlBasicCall sqlBasicCall = (SqlBasicCall) selectNode;
            for(int i=0; i<sqlBasicCall.getOperands().length; i++){
                SqlNode sqlNode = sqlBasicCall.getOperands()[i];
                if(sqlNode instanceof SqlLiteral){
                    continue;
                }

                if(sqlNode instanceof SqlDataTypeSpec){
                    continue;
                }

                SqlNode replaceNode = replaceSelectFieldName(sqlNode, oldTbName, newTbName, mappingField);
                if(replaceNode == null){
                    continue;
                }

                sqlBasicCall.getOperands()[i] = replaceNode;
            }

            return selectNode;
        }else if(selectNode.getKind() == CASE){
            System.out.println("selectNode");
            SqlCase sqlCase = (SqlCase) selectNode;
            SqlNodeList whenOperands = sqlCase.getWhenOperands();
            SqlNodeList thenOperands = sqlCase.getThenOperands();
            SqlNode elseNode = sqlCase.getElseOperand();

            for(int i=0; i<whenOperands.size(); i++){
                SqlNode oneOperand = whenOperands.get(i);
                SqlNode replaceNode = replaceSelectFieldName(oneOperand, oldTbName, newTbName, mappingField);
                if (replaceNode != null) {
                    whenOperands.set(i, replaceNode);
                }
            }

            for(int i=0; i<thenOperands.size(); i++){
                SqlNode oneOperand = thenOperands.get(i);
                SqlNode replaceNode = replaceSelectFieldName(oneOperand, oldTbName, newTbName, mappingField);
                if (replaceNode != null) {
                    thenOperands.set(i, replaceNode);
                }
            }

            ((SqlCase) selectNode).setOperand(3, replaceSelectFieldName(elseNode, oldTbName, newTbName, mappingField));
            return selectNode;
        }else if(selectNode.getKind() == OTHER){
            //不处理
            return selectNode;
        }else{
            throw new RuntimeException(String.format("not support node kind of %s to replace name now.", selectNode.getKind()));
        }
    }


    public static List<SqlNode> replaceSelectStarFieldName(SqlNode selectNode, FieldReplaceInfo replaceInfo){
        SqlIdentifier sqlIdentifier = (SqlIdentifier) selectNode;
        List<SqlNode> sqlNodes = Lists.newArrayList();
        if(sqlIdentifier.isStar()){//处理 [* or table.*]
            int identifierSize = sqlIdentifier.names.size();
            Collection<String> columns = null;
            if(identifierSize == 1){
                columns = replaceInfo.getMappingTable().values();
            }else{
                columns = replaceInfo.getMappingTable().row(sqlIdentifier.names.get(0)).values();
            }

            for(String colAlias : columns){
                SqlParserPos sqlParserPos = new SqlParserPos(0, 0);
                List<String> columnInfo = Lists.newArrayList();
                columnInfo.add(replaceInfo.getTargetTableAlias());
                columnInfo.add(colAlias);
                SqlIdentifier sqlIdentifierAlias = new SqlIdentifier(columnInfo, sqlParserPos);
                sqlNodes.add(sqlIdentifierAlias);
            }

            return sqlNodes;
        }else{
            throw new RuntimeException("is not a star select field." + selectNode);
        }
    }

}
