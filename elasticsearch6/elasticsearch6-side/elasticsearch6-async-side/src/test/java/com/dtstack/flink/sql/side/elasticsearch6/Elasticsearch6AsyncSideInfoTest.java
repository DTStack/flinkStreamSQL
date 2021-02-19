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
public class Elasticsearch6AsyncSideInfoTest {

    @Mock
    RowTypeInfo rowTypeInfo;
    List<FieldInfo> outFieldInfoList = new ArrayList<>();

    AbstractSideTableInfo sideTableInfo = new AbstractSideTableInfo() {
        private static final long serialVersionUID = 8709300729625942851L;

        @Override
        public boolean check() {
            return false;
        }
    };
    @Mock
    TypeInformation typeInformation;
    FieldInfo fieldInfo = new FieldInfo();
    Elasticsearch6AsyncSideInfo sideInfo;

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
        sideTableInfo.addField("mytable");
        String[] str = new String[10];
        str[0] = "varchar";
        sideTableInfo.setFieldTypes(str);
        SqlNode sqlNode = mock(SqlNode.class);
        joinInfo.setCondition(sqlNode);
        /*  Whitebox.setInternalState(sqlKind, OTHER, );*/
        try {
            sideInfo = new Elasticsearch6AsyncSideInfo(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo);
        } catch (Exception ignored) {
        }

    }

    @Test
    public void Test() {

    }
}
