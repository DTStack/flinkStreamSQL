package com.dtstack.flink.sql.side.hbase;

import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.BaseSideInfo;
import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.hbase.table.HbaseSideTableInfo;
import com.dtstack.flink.sql.util.ParseUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

import java.util.List;
import java.util.Map;

/**
 * Reason:
 * Date: 2018/9/18
 * Company: www.dtstack.com
 *
 * @author xuchao
 */

public class HbaseAsyncSideInfo extends BaseSideInfo {

    private static final long serialVersionUID = 257688427401088045L;

    private RowKeyBuilder rowKeyBuilder;

    private Map<String, String> colRefType;

    public HbaseAsyncSideInfo(AbstractSideTableInfo sideTableInfo, String[] lookupKeys) {
        super(sideTableInfo, lookupKeys);
    }

    public HbaseAsyncSideInfo(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, AbstractSideTableInfo sideTableInfo) {
        super(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);
    }

    @Override
    public void parseSelectFields(JoinInfo joinInfo) {
        String sideTableName = joinInfo.getSideTableName();
        String nonSideTableName = joinInfo.getNonSideTable();
        List<String> fields = Lists.newArrayList();
        int sideTableFieldIndex = 0;

        for( int i=0; i<outFieldInfoList.size(); i++){
            FieldInfo fieldInfo = outFieldInfoList.get(i);
            if(fieldInfo.getTable().equalsIgnoreCase(sideTableName)){
                String sideFieldName = sideTableInfo.getPhysicalFields().getOrDefault(fieldInfo.getFieldName(), fieldInfo.getFieldName());
                fields.add(sideFieldName);
                sideSelectFieldsType.put(sideTableFieldIndex, getTargetFieldType(fieldInfo.getFieldName()));
                sideFieldIndex.put(i, sideTableFieldIndex);
                sideFieldNameIndex.put(i, sideFieldName);
                sideTableFieldIndex++;
            }else if(fieldInfo.getTable().equalsIgnoreCase(nonSideTableName)){
                int nonSideIndex = rowTypeInfo.getFieldIndex(fieldInfo.getFieldName());
                inFieldIndex.put(i, nonSideIndex);
            }else{
                throw new RuntimeException("unknown table " + fieldInfo.getTable());
            }
        }

        sideSelectFields = String.join(",", fields);
    }

    @Override
    public void buildEqualInfo(JoinInfo joinInfo, AbstractSideTableInfo sideTableInfo) {
        rowKeyBuilder = new RowKeyBuilder();
        if (sideTableInfo.getPrimaryKeys().size() < 1) {
            throw new RuntimeException("Primary key dimension table must be filled");
        }

        HbaseSideTableInfo hbaseSideTableInfo = (HbaseSideTableInfo) sideTableInfo;
        rowKeyBuilder.init(sideTableInfo.getPrimaryKeys().get(0), sideTableInfo);

        colRefType = Maps.newHashMap();
        for (int i = 0; i < hbaseSideTableInfo.getColumnRealNames().length; i++) {
            String realColName = hbaseSideTableInfo.getColumnRealNames()[i];
            String colType = hbaseSideTableInfo.getFieldTypes()[i];
            colRefType.put(realColName, colType);
        }

        String sideTableName = joinInfo.getSideTableName();
        SqlNode conditionNode = joinInfo.getCondition();

        List<SqlNode> sqlNodeList = Lists.newArrayList();
        ParseUtils.parseAnd(conditionNode, sqlNodeList);

        for (SqlNode sqlNode : sqlNodeList) {
            dealOneEqualCon(sqlNode, sideTableName);
        }

    }

    @Override
    public void buildEqualInfo(AbstractSideTableInfo sideTableInfo) {
        rowKeyBuilder = new RowKeyBuilder();
        if (sideTableInfo.getPrimaryKeys().size() < 1) {
            throw new RuntimeException("Primary key dimension table must be filled");
        }

        HbaseSideTableInfo hbaseSideTableInfo = (HbaseSideTableInfo) sideTableInfo;
        rowKeyBuilder.init(sideTableInfo.getPrimaryKeys().get(0), sideTableInfo);

        colRefType = Maps.newHashMap();
        for (int i = 0; i < hbaseSideTableInfo.getColumnRealNames().length; i++) {
            String realColName = hbaseSideTableInfo.getColumnRealNames()[i];
            String colType = hbaseSideTableInfo.getFieldTypes()[i];
            colRefType.put(realColName, colType);
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
