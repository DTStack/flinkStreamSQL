package com.dtstack.flink.sql.side.elasticsearch6;


import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;

/**
 * Company: www.dtstack.com
 *
 * @author zhufeng
 * @date 2020-07-02
 */
public class Elasticsearch6AllSideInfoTest {
    FieldInfo fieldInfo = new FieldInfo();
    @Mock
    TypeInformation typeInformation;
    List<FieldInfo> outFieldInfoList = new ArrayList<FieldInfo>();
    Elasticsearch6AllSideInfo allSideInfo;
    @Mock
    RowTypeInfo rowTypeInfo;
    @Mock
    AbstractSideTableInfo sideTableInfo;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        fieldInfo = new FieldInfo();
        fieldInfo.setTypeInformation(typeInformation);
        fieldInfo.setTable("mytable");
        fieldInfo.setFieldName("mytable");
        outFieldInfoList.add(0, fieldInfo);
        JoinInfo joinInfo = new JoinInfo();
        joinInfo.setLeftTableAlias("mytable");
        joinInfo.setLeftIsSideTable(true);
        SqlNode sqlNode = mock(SqlNode.class);
        joinInfo.setCondition(sqlNode);
        try {
            allSideInfo = new Elasticsearch6AllSideInfo(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);
        } catch (Exception ignored) {
        }
        JoinInfo joinInfo1 = new JoinInfo();
        joinInfo1.setRightTableAlias("mytable");
        joinInfo1.setLeftIsSideTable(true);
        try {
            allSideInfo = new Elasticsearch6AllSideInfo(rowTypeInfo, joinInfo1, outFieldInfoList, sideTableInfo);
        } catch (Exception ignored) {
        }
        JoinInfo joinInfo2 = new JoinInfo();
        try {
            allSideInfo = new Elasticsearch6AllSideInfo(rowTypeInfo, joinInfo2, outFieldInfoList, sideTableInfo);
        } catch (Exception ignored) {
        }
    }

    @Test
    public void buildEqualInfoTest() {
    }
}
