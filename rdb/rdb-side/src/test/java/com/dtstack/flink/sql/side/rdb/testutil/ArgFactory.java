package com.dtstack.flink.sql.side.rdb.testutil;

import com.dtstack.flink.sql.side.BaseSideInfo;
import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.rdb.async.RdbAsyncSideInfo;
import com.dtstack.flink.sql.side.rdb.table.RdbSideTableInfo;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.Types;

import java.util.*;

/**
 * @program: flink.sql
 * @author: wuren
 * @create: 2020/07/30
 **/
public class ArgFactory {

    public static List<FieldInfo> genOutFieldInfoList() {
        List<FieldInfo> outFieldInfoList = new ArrayList<>();
        FieldInfo fieldInfoa = new FieldInfo();
        fieldInfoa.setTable("x");
        fieldInfoa.setFieldName("id");
        fieldInfoa.setTypeInformation(TypeInformation.of(Integer.class));
        outFieldInfoList.add(fieldInfoa);
        FieldInfo fieldInfob = new FieldInfo();
        fieldInfob.setTable("x");
        fieldInfob.setFieldName("name");
        fieldInfob.setTypeInformation(TypeInformation.of(String.class));
        outFieldInfoList.add(fieldInfob);
        FieldInfo fieldInfoc = new FieldInfo();
        fieldInfoc.setTable("y");
        fieldInfoc.setFieldName("id");
        fieldInfoc.setTypeInformation(TypeInformation.of(Integer.class));
        outFieldInfoList.add(fieldInfoc);
        FieldInfo fieldInfod = new FieldInfo();
        fieldInfod.setTable("y");
        fieldInfod.setFieldName("name");
        fieldInfod.setTypeInformation(TypeInformation.of(String.class));
        outFieldInfoList.add(fieldInfod);
        return outFieldInfoList;
    }

    public static JoinInfo genJoinInfo() {
        String sql = "select \n" +
            "   x.id \n" +
            "   ,y.id \n" +
            "  from TEST_ods x left join TEST_dim y \n" +
            "  on x.id = y.id";

//        String sql = "select \n" +
//            "   m.id as mid \n" +
//            "   ,m.bb mbb \n" +
//            "   ,s.channel as sid\n" +
//            "   ,s.name as sbb \n" +
//            "  from MyTable m left join hbaseSide s\n" +
//            "  on m.id = s.rowkey";
        SqlParser.Config config = SqlParser
            .configBuilder()
            .setLex(Lex.MYSQL)
            .build();
        SqlParser sqlParser = SqlParser.create(sql, config);
        SqlNode sqlNode = null;
        try {
            sqlNode = sqlParser.parseStmt();
        } catch (SqlParseException e) {
            throw new RuntimeException("", e);
        }

        JoinInfo joinInfo = new JoinInfo();
        joinInfo.setLeftIsSideTable(false);
        joinInfo.setRightIsSideTable(true);
        final String LEFT_TABLE_NAME = "TEST_ods";
        final String RIGHT_TABLE_NAME = "TEST_dim";
        joinInfo.setRightTableAlias(RIGHT_TABLE_NAME);
        joinInfo.setLeftTableAlias(LEFT_TABLE_NAME);
        joinInfo.setLeftTableAlias("x");
        joinInfo.setRightTableAlias("y");
        joinInfo.setLeftNode(((SqlJoin) ((SqlSelect) sqlNode).getFrom()).getLeft());
        joinInfo.setRightNode(((SqlJoin) ((SqlSelect) sqlNode).getFrom()).getRight());
        joinInfo.setCondition(((SqlJoin) ((SqlSelect) sqlNode).getFrom()).getCondition());
        joinInfo.setSelectFields(((SqlSelect) sqlNode).getSelectList());
        joinInfo.setSelectNode(((SqlSelect) sqlNode).getSelectList());
        joinInfo.setJoinType(((SqlJoin) ((SqlSelect) sqlNode).getFrom()).getJoinType());
        joinInfo.setScope("0");
        Map leftSelectField = new HashMap<String, String>();
        leftSelectField.put("name", "name");
        leftSelectField.put("id", "id");
        joinInfo.setLeftSelectFieldInfo(leftSelectField);
        Map rightSelectField = new HashMap<String, String>();
        rightSelectField.put("id", "id");
        rightSelectField.put("name", "name");
        joinInfo.setRightSelectFieldInfo(rightSelectField);
        return joinInfo;
    }

    public static RdbSideTableInfo genSideTableInfo() {
        RdbSideTableInfo sideTableInfo = new RdbSideTableInfo();
        sideTableInfo.setTableName("TEST_dim");
        sideTableInfo.setName("TEST_dim");
        sideTableInfo.setType("mysql");
        sideTableInfo.setFields(new String[]{"id", "name"});
        sideTableInfo.setFieldTypes(new String[]{"varchar", "varchar"});
        sideTableInfo.setFieldClasses(new Class[]{Integer.class, String.class});

        sideTableInfo.addField("id");
        sideTableInfo.addField("name");
        sideTableInfo.addFieldType("int");
        sideTableInfo.addFieldType("varchar");
        sideTableInfo.addFieldClass(Integer.class);
        sideTableInfo.addFieldClass(String.class);
        return sideTableInfo;
    }

    public static RowTypeInfo genRowTypeInfo() {
        TypeInformation<?>[] types = new TypeInformation[] {
            Types.INT(),
            Types.STRING()
        };
        String[] fieldNames = new String[] {
            "id",
            "name"
        };
        RowTypeInfo rowTypeInfo = new RowTypeInfo(types, fieldNames);
        return rowTypeInfo;
    }

    public static BaseSideInfo getSideInfo() {
        return new RdbAsyncSideInfo(genRowTypeInfo(), genJoinInfo(), genOutFieldInfoList(), genSideTableInfo());
    }
}
