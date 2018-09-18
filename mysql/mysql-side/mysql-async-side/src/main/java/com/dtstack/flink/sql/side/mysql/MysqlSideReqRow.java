package com.dtstack.flink.sql.side.mysql;

import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.SideReqRow;
import com.dtstack.flink.sql.side.SideTableInfo;
import com.dtstack.flink.sql.side.mysql.table.MysqlSideTableInfo;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;

import java.util.List;

/**
 * Reason:
 * Date: 2018/9/18
 * Company: www.dtstack.com
 *
 * @author xuchao
 */

public class MysqlSideReqRow extends SideReqRow {

    public MysqlSideReqRow(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) {
        super(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);
    }

    @Override
    public void buildEqualInfo(JoinInfo joinInfo, SideTableInfo sideTableInfo) {
        MysqlSideTableInfo mysqlSideTableInfo = (MysqlSideTableInfo) sideTableInfo;

        String sideTableName = joinInfo.getSideTableName();

        SqlNode conditionNode = joinInfo.getCondition();

        List<SqlNode> sqlNodeList = Lists.newArrayList();
        if(conditionNode.getKind() == SqlKind.AND){
            sqlNodeList.addAll(Lists.newArrayList(((SqlBasicCall)conditionNode).getOperands()));
        }else{
            sqlNodeList.add(conditionNode);
        }

        for(SqlNode sqlNode : sqlNodeList){
            dealOneEqualCon(sqlNode, sideTableName);
        }

        sqlCondition = "select ${selectField} from ${tableName} where ";
        for(int i=0; i<equalFieldList.size(); i++){
            String equalField = equalFieldList.get(i);

            sqlCondition += equalField + "=? ";
            if(i != equalFieldList.size() - 1){
                sqlCondition += " and ";
            }
        }

        sqlCondition = sqlCondition.replace("${tableName}", mysqlSideTableInfo.getTableName()).replace("${selectField}", sideSelectFields);
        System.out.println("---------side_exe_sql-----\n" + sqlCondition);
    }

    @Override
    public void parseSelectFields(JoinInfo joinInfo){
        String sideTableName = joinInfo.getSideTableName();
        String nonSideTableName = joinInfo.getNonSideTable();
        List<String> fields = Lists.newArrayList();

        int sideIndex = 0;
        for( int i=0; i<outFieldInfoList.size(); i++){
            FieldInfo fieldInfo = outFieldInfoList.get(i);
            if(fieldInfo.getTable().equalsIgnoreCase(sideTableName)){
                fields.add(fieldInfo.getFieldName());
                sideFieldIndex.put(i, sideIndex);
                sideIndex++;
            }else if(fieldInfo.getTable().equalsIgnoreCase(nonSideTableName)){
                int nonSideIndex = rowTypeInfo.getFieldIndex(fieldInfo.getFieldName());
                inFieldIndex.put(i, nonSideIndex);
            }else{
                throw new RuntimeException("unknown table " + fieldInfo.getTable());
            }
        }

        if(fields.size() == 0){
            throw new RuntimeException("select non field from table " +  sideTableName);
        }

        sideSelectFields = String.join(",", fields);
    }


    @Override
    public void dealOneEqualCon(SqlNode sqlNode, String sideTableName){
        if(sqlNode.getKind() != SqlKind.EQUALS){
            throw new RuntimeException("not equal operator.");
        }

        SqlIdentifier left = (SqlIdentifier)((SqlBasicCall)sqlNode).getOperands()[0];
        SqlIdentifier right = (SqlIdentifier)((SqlBasicCall)sqlNode).getOperands()[1];

        String leftTableName = left.getComponent(0).getSimple();
        String leftField = left.getComponent(1).getSimple();

        String rightTableName = right.getComponent(0).getSimple();
        String rightField = right.getComponent(1).getSimple();

        if(leftTableName.equalsIgnoreCase(sideTableName)){
            equalFieldList.add(leftField);
            int equalFieldIndex = -1;
            for(int i=0; i<rowTypeInfo.getFieldNames().length; i++){
                String fieldName = rowTypeInfo.getFieldNames()[i];
                if(fieldName.equalsIgnoreCase(rightField)){
                    equalFieldIndex = i;
                }
            }
            if(equalFieldIndex == -1){
                throw new RuntimeException("can't deal equal field: " + sqlNode);
            }

            equalValIndex.add(equalFieldIndex);

        }else if(rightTableName.equalsIgnoreCase(sideTableName)){

            equalFieldList.add(rightField);
            int equalFieldIndex = -1;
            for(int i=0; i<rowTypeInfo.getFieldNames().length; i++){
                String fieldName = rowTypeInfo.getFieldNames()[i];
                if(fieldName.equalsIgnoreCase(leftField)){
                    equalFieldIndex = i;
                }
            }
            if(equalFieldIndex == -1){
                throw new RuntimeException("can't deal equal field: " + sqlNode.toString());
            }

            equalValIndex.add(equalFieldIndex);

        }else{
            throw new RuntimeException("resolve equalFieldList error:" + sqlNode.toString());
        }

    }

}
