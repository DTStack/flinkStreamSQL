package com.dtstack.flink.sql.util;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.shaded.guava18.com.google.common.base.Splitter;
import org.apache.flink.table.api.Types;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @program: flink.sql
 * @author: wuren
 * @create: 2020/07/09
 **/
public class DataTypeUtils {

    private final static Pattern COMPOSITE_TYPE_PATTERN = Pattern.compile("(.+?)<(.+)>");
    private final static String ARRAY = "ARRAY";
    private final static String ROW = "ROW";
    private final static char FIELD_DELIMITER = ',';
    private final static char TYPE_DELIMITER = ' ';

    private DataTypeUtils() {}

    public static TypeInformation convertToCompositeType(String string) {
        Matcher matcher = COMPOSITE_TYPE_PATTERN.matcher(string);
        // TODO 现在只支持ARRAY类型后续可以加入 MAP等类型
        if (matcher.find() && ARRAY.equals(matcher.group(1))) {
            return convertToArray(string);
        } else {
            throw new RuntimeException("type " + string + "is not support!");
        }
    }

    /**
     * 目前ARRAY里只支持ROW和其他基本类型
     * @param arrayTypeString
     * @return
     */
    public static TypeInformation convertToArray(String arrayTypeString) {
        Matcher matcher = COMPOSITE_TYPE_PATTERN.matcher(arrayTypeString);
        if (matcher.find() && ARRAY.equals(matcher.group(1))) {
            String elementTypeString = matcher.group(2);
            TypeInformation elementType;
            if (elementTypeString.toUpperCase().startsWith(ROW)) {
                elementType = convertToRow(elementTypeString);
            } else {
                elementType = convertToAtomicType(elementTypeString);
            }
            return Types.OBJECT_ARRAY(elementType);
        } else {
            throw new RuntimeException(arrayTypeString + "convert to array type error!");
        }

    }

    /**
     * 目前ROW里只支持基本类型
     * @param string
     */
    public static RowTypeInfo convertToRow(String string) {
        Matcher matcher = COMPOSITE_TYPE_PATTERN.matcher(string);

        if (matcher.find() &&
            ROW.equals(matcher.group(1).toUpperCase())
        ) {
            String elementTypeStr = matcher.group(2);
            Iterable<String> typeInfo = splitCompositeTypeFields(elementTypeStr);
            Tuple2<TypeInformation[], String[]> tuple = genFieldInfo(typeInfo);
            return new RowTypeInfo(tuple.f0, tuple.f1);
        } else {
            throw new RuntimeException(string + "convert to row type error!");
        }
    }

    private static Iterable<String> splitCompositeTypeFields(String string) {
        return Splitter
            .on(FIELD_DELIMITER)
            .trimResults()
            .split(string);
    }

    private static Tuple2<TypeInformation[], String[]> genFieldInfo(Iterable<String> typeInfo) {
        ArrayList<TypeInformation> types = Lists.newArrayList();
        ArrayList<String> fieldNames = Lists.newArrayList();

        for (String type : typeInfo) {
            Iterable<String> fieldInfo = Splitter
                .on(TYPE_DELIMITER)
                .trimResults()
                .omitEmptyStrings()
                .split(type);

            ArrayList<String> array = Lists.newArrayList(fieldInfo.iterator());
            Preconditions.checkState(array.size() == 2, "field info must be name with type");
            TypeInformation fieldType = convertToAtomicType(array.get(1));
            types.add(fieldType);
            fieldNames.add(array.get(0));
        }

        TypeInformation[] typeArray = types.toArray(new TypeInformation[types.size()]);
        String[] fieldNameArray = fieldNames.toArray(new String[fieldNames.size()]);
        return Tuple2.of(typeArray, fieldNameArray);
    }

    /**
     * 转换基本类型，所有类型参考Flink官方文档，一共12个基本类型。
     * @param string
     * @return
     */
    public static TypeInformation convertToAtomicType(String string) {
        switch (string.toUpperCase()) {
            case "VARCHAR":
            case "STRING":
                return Types.STRING();
            case "BOOLEAN":
                return Types.BOOLEAN();
            case "TINYINT":
                return Types.BYTE();
            case "SMALLINT":
                return Types.SHORT();
            case "INT":
            case "INTEGER":
                return Types.INT();
            case "BIGINT":
                return Types.LONG();
            case "FLOAT":
            case "REAL":
                return Types.FLOAT();
            case "DOUBLE":
                return Types.DOUBLE();
            case "DECIMAL":
            case "DEC":
            case "NUMERIC":
                return Types.DECIMAL();
            case "DATE":
                return Types.SQL_DATE();
            case "TIME":
                return Types.SQL_TIME();
            case "TIMESTAMP":
                return Types.SQL_TIMESTAMP();
            default:
                throw new RuntimeException("type " + string + "not supported, please refer to the flink doc!");
        }
    }

}
