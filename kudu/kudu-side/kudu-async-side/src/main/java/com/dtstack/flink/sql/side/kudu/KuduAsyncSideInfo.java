package com.dtstack.flink.sql.side.kudu;

import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.BaseSideInfo;
import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.kudu.table.KuduSideTableInfo;
import com.dtstack.flink.sql.util.ParseUtils;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

import java.util.List;

public class KuduAsyncSideInfo extends BaseSideInfo {


    public KuduAsyncSideInfo(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, AbstractSideTableInfo sideTableInfo) {
        super(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);
    }

    public KuduAsyncSideInfo(AbstractSideTableInfo sideTableInfo, String[] lookupKeys) {
        super(sideTableInfo, lookupKeys);
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
    public void buildEqualInfo(AbstractSideTableInfo sideTableInfo) {
        super.buildEqualInfo(sideTableInfo);
    }

}
