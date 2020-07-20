package com.dtstack.flink.sql.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Array;

import static org.junit.Assert.*;

public class DataTypeUtilsTest {

    @Test
    public void convertToArray() {
        String[] normalFieldNames = new String[] { "id", "name" };
        TypeInformation[] normalTypes = new TypeInformation[] {Types.INT(), Types.STRING()};
        RowTypeInfo rowTypeInfo = new RowTypeInfo(normalTypes, normalFieldNames);
        TypeInformation normalAtomicType = Types.OBJECT_ARRAY(Types.STRING());
        TypeInformation normalCompositeType = Types.OBJECT_ARRAY(rowTypeInfo);

        String atomicStrWithBlank = " ARRAY< STRING\n > ";
        String compositeTypeStrWithBlank = " ARRAY<  ROW< id  INT, name  STRING >  > ";

        TypeInformation atomicArrayTypeWithBlank = DataTypeUtils.convertToArray(atomicStrWithBlank);
        TypeInformation compositeTypeArrayTypeWithBlank = DataTypeUtils.convertToArray(compositeTypeStrWithBlank);

        Assert.assertTrue(normalAtomicType.equals(atomicArrayTypeWithBlank));
        Assert.assertTrue(normalCompositeType.equals(compositeTypeArrayTypeWithBlank));


        String atomicStr = "ARRAY<STRING>";
        String compositeTypeStr = "ARRAY<ROW<id INT, name STRING>>";

        TypeInformation atomicArrayType = DataTypeUtils.convertToArray(atomicStr);
        TypeInformation compositeTypeArrayType = DataTypeUtils.convertToArray(compositeTypeStr);

        Assert.assertTrue(normalAtomicType.equals(atomicArrayType));
        Assert.assertTrue(normalCompositeType.equals(compositeTypeArrayType));
    }

    @Test
    public void convertToRow() {
        String string = " ROW < id INT, name STRING > ";
        RowTypeInfo rowType = DataTypeUtils.convertToRow(string);

        String[] fieldNames = rowType.getFieldNames();
        Assert.assertTrue("id".equals(fieldNames[0]));
        Assert.assertTrue("name".equals(fieldNames[1]));

        TypeInformation[] fieldTypes = rowType.getFieldTypes();
        Assert.assertTrue(Types.INT() == fieldTypes[0]);
        Assert.assertTrue(Types.STRING() == fieldTypes[1]);
    }

    @Test
    public void convertToAtomicType() {
        TypeInformation type = DataTypeUtils.convertToAtomicType(" TIMESTAMP ");
        Assert.assertTrue(type == Types.SQL_TIMESTAMP());
    }
}