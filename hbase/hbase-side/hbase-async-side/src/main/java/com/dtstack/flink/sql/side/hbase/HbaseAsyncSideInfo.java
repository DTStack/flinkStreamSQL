package com.dtstack.flink.sql.side.hbase;

import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.SideInfo;
import com.dtstack.flink.sql.side.SideTableInfo;
import com.dtstack.flink.sql.side.hbase.table.HbaseSideTableInfo;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * Reason:
 * Date: 2018/9/18
 * Company: www.dtstack.com
 * @author xuchao
 */

public class HbaseAsyncSideInfo extends SideInfo {

    private static final long serialVersionUID = 257688427401088045L;

    private RowKeyBuilder rowKeyBuilder;

    private Map<String, String> colRefType;

    public HbaseAsyncSideInfo(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) {
        super(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);
    }

    @Override
    public void buildEqualInfo(JoinInfo joinInfo, SideTableInfo sideTableInfo) {
        rowKeyBuilder = new RowKeyBuilder();
        if(sideTableInfo.getPrimaryKeys().size() < 1){
            throw new RuntimeException("Primary key dimension table must be filled");
        }

        HbaseSideTableInfo hbaseSideTableInfo = (HbaseSideTableInfo) sideTableInfo;
        rowKeyBuilder.init(sideTableInfo.getPrimaryKeys().get(0));

        colRefType = Maps.newHashMap();
        for(int i=0; i<hbaseSideTableInfo.getColumnRealNames().length; i++){
            String realColName = hbaseSideTableInfo.getColumnRealNames()[i];
            String colType = hbaseSideTableInfo.getFieldTypes()[i];
            colRefType.put(realColName, colType);
        }

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

    }

    public RowKeyBuilder getRowKeyBuilder() {
        return rowKeyBuilder;
    }

    public void setRowKeyBuilder(RowKeyBuilder rowKeyBuilder) {
        this.rowKeyBuilder = rowKeyBuilder;
    }

    public Map<String, String> getColRefType() {
        return colRefType;
    }

    public void setColRefType(Map<String, String> colRefType) {
        this.colRefType = colRefType;
    }
}
