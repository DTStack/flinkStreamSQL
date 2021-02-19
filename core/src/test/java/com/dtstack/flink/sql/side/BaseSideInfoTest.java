package com.dtstack.flink.sql.side;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BaseSideInfoTest {

    private BaseSideInfo sideInfo;

    private AbstractSideTableInfo sideTableInfo;

    @Before
    public void init(){

        RowTypeInfo rowTypeInfo = mock(RowTypeInfo.class);
        when(rowTypeInfo.getFieldNames()).thenReturn(new String[]{"a"});
        JoinInfo joinInfo = mock(JoinInfo.class);
        when(joinInfo.getSideTableName()).thenReturn("a");
        when(joinInfo.getNonSideTable()).thenReturn("b");

        FieldInfo fieldInfo = new FieldInfo();
        fieldInfo.setTable("a");
        fieldInfo.setFieldName("a");

        List<FieldInfo> outFieldInfoList = Lists.newArrayList(fieldInfo);
        sideTableInfo = mock(AbstractSideTableInfo.class);
        when(sideTableInfo.getFieldList()).thenReturn(Lists.newArrayList("a"));
        when(sideTableInfo.getFieldTypes()).thenReturn(new String[]{"string"});

        when(sideTableInfo.getPhysicalFields()).thenReturn(Maps.newHashMap());
        sideInfo = new BaseSideInfo(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo) {
            @Override
            public void buildEqualInfo(JoinInfo joinInfo, AbstractSideTableInfo sideTableInfo) {

            }
        };

    }

    @Test
    public void dealOneEqualCon(){
        SqlBasicCall sqlNode = mock(SqlBasicCall.class);
        when(sqlNode.getKind()).thenReturn(SqlKind.NOT_EQUALS);


        SqlIdentifier sqlIdentifier1 = mock(SqlIdentifier.class);
        when(sqlIdentifier1.getSimple()).thenReturn("a");

        SqlIdentifier sqlIdentifier2 = mock(SqlIdentifier.class);
        when(sqlIdentifier2.getSimple()).thenReturn("b");

        when(sqlIdentifier1.getComponent(0)).thenReturn(sqlIdentifier1);
        when(sqlIdentifier1.getComponent(1)).thenReturn(sqlIdentifier2);
        when(sqlIdentifier2.getComponent(0)).thenReturn(sqlIdentifier2);
        when(sqlIdentifier2.getComponent(1)).thenReturn(sqlIdentifier1);
        when(sqlNode.getOperands()).thenReturn(new SqlNode[]{sqlIdentifier1, sqlIdentifier2});

        sideInfo.dealOneEqualCon(sqlNode, "a");

        try {
            sideInfo.dealOneEqualCon(sqlNode, "b");
        } catch (Exception e){

        }

        sideInfo.setSideCache(null);
        sideInfo.setSideTableInfo(null);
        sideInfo.setEqualFieldList(null);
        sideInfo.setEqualValIndex(null);
        sideInfo.setInFieldIndex(null);
        sideInfo.setJoinType(JoinType.LEFT);
        sideInfo.setSqlCondition("");
        sideInfo.setSideSelectFields("");
        sideInfo.setOutFieldInfoList(null);
        sideInfo.setSideFieldNameIndex(null);
        sideInfo.setSideSelectFieldsType(null);
        sideInfo.setRowTypeInfo(null);
        sideInfo.setSideSelectFields("");
    }





}
