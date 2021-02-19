package com.dtstack.flink.sql.side.rdb.async;

import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.PredicateInfo;
import com.dtstack.flink.sql.side.rdb.table.RdbSideTableInfo;
import com.dtstack.flink.sql.side.rdb.testutil.ArgFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.Types;
import org.junit.Before;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

public class RdbAsyncSideInfoTest {

    RdbAsyncSideInfo sideInfo;

    @Before
    public void setUp() {

        sideInfo = Whitebox.newInstance(RdbAsyncSideInfo.class);

        sideInfo = new RdbAsyncSideInfo(
            ArgFactory.genRowTypeInfo(),
            ArgFactory.genJoinInfo(),
            ArgFactory.genOutFieldInfoList(),
            ArgFactory.genSideTableInfo()
        );

    }

    @Test
    public void testBuildEqualInfo() {
        JoinInfo joinInfo = ArgFactory.genJoinInfo();
        joinInfo.setLeftIsSideTable(true);
        sideInfo.buildEqualInfo(joinInfo, ArgFactory.genSideTableInfo());
    }

    @Test
    public void testBuildFilterCondition() {
        final String operatorName = "";
        final String ownerTable = "ods_test";
        final String fieldName = "id";
        final String condition = "";

        final String NOT_IN = "NOT_IN";
        final String NOT_EQUALS = "NOT_EQUALS";
        final String BETWEEN = "BETWEEN";
        final String IS_NULL = "IS_NULL";
        final String DEFAULT = "";

        PredicateInfo predicateInfo = new PredicateInfo(operatorName, NOT_IN, ownerTable, fieldName, condition);
        String notNullCondition = sideInfo.buildFilterCondition(predicateInfo);
        predicateInfo.setOperatorKind(NOT_EQUALS);
        String notEqualsCondition = sideInfo.buildFilterCondition(predicateInfo);
        predicateInfo.setOperatorKind(BETWEEN);
        String betweenCondition = sideInfo.buildFilterCondition(predicateInfo);
        predicateInfo.setOperatorKind(IS_NULL);
        String isNullCondition = sideInfo.buildFilterCondition(predicateInfo);
        predicateInfo.setOperatorKind(DEFAULT);
        String defaultCondition = sideInfo.buildFilterCondition(predicateInfo);

    }

}