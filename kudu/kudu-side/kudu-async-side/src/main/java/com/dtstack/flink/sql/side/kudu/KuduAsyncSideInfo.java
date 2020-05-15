package com.dtstack.flink.sql.side.kudu;

import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.BaseSideInfo;
import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.kudu.table.KuduSideTableInfo;
import com.dtstack.flink.sql.util.ParseUtils;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

import java.util.List;

public class KuduAsyncSideInfo extends BaseSideInfo {


    public KuduAsyncSideInfo(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, AbstractSideTableInfo sideTableInfo) {
        super(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);
    }

    @Override
    public void buildEqualInfo(JoinInfo joinInfo, AbstractSideTableInfo sideTableInfo) {
        KuduSideTableInfo kuduSideTableInfo = (KuduSideTableInfo) sideTableInfo;

        String sideTableName = joinInfo.getSideTableName();

        SqlNode conditionNode = joinInfo.getCondition();

        List<SqlNode> sqlNodeList = Lists.newArrayList();
        ParseUtils.parseAnd(conditionNode, sqlNodeList);

        for (SqlNode sqlNode : sqlNodeList) {
            dealOneEqualCon(sqlNode, sideTableName);
        }

        sqlCondition = "select ${selectField} from ${tableName} ";
        sqlCondition = sqlCondition.replace("${tableName}", kuduSideTableInfo.getTableName()).replace("${selectField}", sideSelectFields);
        System.out.println("---------side_exe_sql-----\n" + sqlCondition);
    }

    @Override
    public void dealOneEqualCon(SqlNode sqlNode, String sideTableName) {
        if (sqlNode.getKind() != SqlKind.EQUALS) {
            throw new RuntimeException("not equal operator.");
        }

        SqlIdentifier left = (SqlIdentifier) ((SqlBasicCall) sqlNode).getOperands()[0];
        SqlIdentifier right = (SqlIdentifier) ((SqlBasicCall) sqlNode).getOperands()[1];

        String leftTableName = left.getComponent(0).getSimple();
        String leftField = left.getComponent(1).getSimple();

        String rightTableName = right.getComponent(0).getSimple();
        String rightField = right.getComponent(1).getSimple();

        if (leftTableName.equalsIgnoreCase(sideTableName)) {
            equalFieldList.add(leftField);
            int equalFieldIndex = -1;
            for (int i = 0; i < rowTypeInfo.getFieldNames().length; i++) {
                String fieldName = rowTypeInfo.getFieldNames()[i];
                if (fieldName.equalsIgnoreCase(rightField)) {
                    equalFieldIndex = i;
                }
            }
            if (equalFieldIndex == -1) {
                throw new RuntimeException("can't deal equal field: " + sqlNode);
            }

            equalValIndex.add(equalFieldIndex);

        } else if (rightTableName.equalsIgnoreCase(sideTableName)) {

            equalFieldList.add(rightField);
            int equalFieldIndex = -1;
            for (int i = 0; i < rowTypeInfo.getFieldNames().length; i++) {
                String fieldName = rowTypeInfo.getFieldNames()[i];
                if (fieldName.equalsIgnoreCase(leftField)) {
                    equalFieldIndex = i;
                }
            }
            if (equalFieldIndex == -1) {
                throw new RuntimeException("can't deal equal field: " + sqlNode.toString());
            }

            equalValIndex.add(equalFieldIndex);

        } else {
            throw new RuntimeException("resolve equalFieldList error:" + sqlNode.toString());
        }

    }
}
