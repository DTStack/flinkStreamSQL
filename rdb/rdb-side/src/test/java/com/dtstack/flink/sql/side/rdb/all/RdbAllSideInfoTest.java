package com.dtstack.flink.sql.side.rdb.all;

import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.PredicateInfo;
import com.dtstack.flink.sql.side.rdb.table.RdbSideTableInfo;
import com.google.common.collect.Maps;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.ArrayList;
import java.util.List;

import static org.powermock.api.support.membermodification.MemberMatcher.method;
import static org.powermock.api.support.membermodification.MemberModifier.suppress;

@RunWith(PowerMockRunner.class)
@PrepareForTest(RdbAllSideInfo.class)
public class RdbAllSideInfoTest {

    RdbAllSideInfo sideInfo;

    @Before
    public void setUp() {
        TypeInformation<?>[] types = new TypeInformation[] {
            Types.INT(),
            Types.STRING()
        };
        String[] fieldNames = new String[] {
            "id",
            "name"
        };
        RowTypeInfo rowTypeInfo = new RowTypeInfo(types, fieldNames);
        sideInfo = Whitebox.newInstance(RdbAllSideInfo.class);
        sideInfo.setRowTypeInfo(rowTypeInfo);
        sideInfo.setInFieldIndex(Maps.newHashMap());
        sideInfo.setSideFieldIndex(Maps.newHashMap());
        sideInfo.setSideFieldNameIndex(Maps.newHashMap());
    }

//    @Test
    public void testBuildEqualInfo() {
        RdbSideTableInfo tableInfo = new RdbSideTableInfo();
        tableInfo.setTableName("TEST_ods");
        List<PredicateInfo> predicateInfoes = new ArrayList<>();
        PredicateInfo info = PredicateInfo.builder()
            .setCondition("1")
            .setOwnerTable("TEST_dwd")
            .setOperatorKind("EQUALS")
            .setOperatorName("=")
            .setFieldName("id")
            .build();
        predicateInfoes.add(info);
        tableInfo.setPredicateInfoes(predicateInfoes);
        sideInfo.setSideSelectFields("id, name");
        sideInfo.buildEqualInfo(null, tableInfo);
        String stmt = sideInfo.getSqlCondition();
        // TODO 需要找到正确的normal值，让后把这个单测case完成。
        final String normal = "SELECT  id ,   name  FROM TEST_ods WHERE  id  = 1";
//        Assert.assertTrue(normal.equals(stmt));
    }

//    @Test
    public void testParseSelectFields() throws SqlParseException {
        JoinInfo joinInfo = new JoinInfo();
        final String LEFT_TABLE_NAME = "TEST_ods";
        final String RIGHT_TABLE_NAME = "TEST_dim";
        joinInfo.setRightTableAlias(RIGHT_TABLE_NAME);
        joinInfo.setLeftTableAlias(LEFT_TABLE_NAME);
        String sql = "select \n" +
            "   x.id \n" +
            "   ,y.id \n" +
            "  from TEST_ods x left join TEST_dim y7 \n" +
            "  on x.id = y.id";
        SqlParser.Config config = SqlParser
            .configBuilder()
            .setLex(Lex.MYSQL)
            .build();
        SqlParser sqlParser = SqlParser.create(sql, config);
        SqlNode sqlNode = sqlParser.parseStmt();

        joinInfo.setCondition(((SqlJoin) ((SqlSelect) sqlNode).getFrom()).getCondition());
        List<FieldInfo> outFieldInfoList = new ArrayList<>();
        sideInfo.setOutFieldInfoList(outFieldInfoList);
        try {
            sideInfo.parseSelectFields(joinInfo);
        } catch (Exception e) {
            final String normalErrorMsg = "select non field from table " + RIGHT_TABLE_NAME;
            String errorMsg = e.getMessage();
            Assert.assertTrue(normalErrorMsg.equals(errorMsg));
        }

        FieldInfo field1 = new FieldInfo();
        FieldInfo field2 = new FieldInfo();
        field1.setTable(LEFT_TABLE_NAME);
        field1.setFieldName("id");
        field1.setTypeInformation(Types.INT());
        field2.setTable(RIGHT_TABLE_NAME);
        field2.setFieldName("name");
        field2.setTypeInformation(Types.STRING());
        outFieldInfoList.add(field1);
        outFieldInfoList.add(field2);
        suppress(method(RdbAllSideInfo.class, "dealOneEqualCon"));
        try {
            sideInfo.parseSelectFields(joinInfo);
        } catch (Exception e) {
            final String normalErrorMsg = "no join condition found after table null";
            String errorMsg = e.getMessage();
            Assert.assertTrue(normalErrorMsg.equals(errorMsg));
        }
        List<String> fieldList = Lists.newArrayList();
        fieldList.add("id");
        fieldList.add("name");
        sideInfo.setEqualFieldList(fieldList);
        sideInfo.parseSelectFields(joinInfo);
        String selectFields = sideInfo.getSideSelectFields();
        final String normalSelectFields = "name,id";
        Assert.assertTrue(normalSelectFields.equals(selectFields));
    }

//    @Test
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

//    @Test
    public void testGetTableName() {
        final String TABLE_NAME = "foo_name";
        RdbSideTableInfo tabInfo = PowerMockito.mock(RdbSideTableInfo.class);
        PowerMockito.when(tabInfo.getTableName()).thenReturn("foo_name");
        String tableName = sideInfo.getTableName(tabInfo);
        Assert.assertTrue(TABLE_NAME.equals(tableName));
    }

//    @Test
    public void testQuoteIdentifier() {
        String quoteStr = sideInfo.quoteIdentifier("foo");
        Assert.assertTrue(" foo ".equals(quoteStr));
    }

}