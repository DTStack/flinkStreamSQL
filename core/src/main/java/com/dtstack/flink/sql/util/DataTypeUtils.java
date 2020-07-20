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

    /**
     * 现在只支持ARRAY类型后续可以加入 MAP等类型
     * @param compositeTypeString
     * @return
     */
    public static TypeInformation convertToCompositeType(String compositeTypeString) {
        Matcher matcher = matchCompositeType(compositeTypeString);
        final String errorMsg = "type " + compositeTypeString + "is not support!";
        Preconditions.checkState(matcher.find(), errorMsg);

        String normalizedType = normalizeType(matcher.group(1));
        Preconditions.checkState(ARRAY.equals(normalizedType), errorMsg);

        return convertToArray(compositeTypeString);
    }

    /**
     * 目前ARRAY里只支持ROW和其他基本类型
     * @param arrayTypeString
     * @return
     */
    public static TypeInformation convertToArray(String arrayTypeString) {
        Matcher matcher = matchCompositeType(arrayTypeString);
        final String errorMsg =  arrayTypeString + "convert to array type error!";
        Preconditions.checkState(matcher.find(), errorMsg);

        String normalizedType = normalizeType(matcher.group(1));
        Preconditions.checkState(ARRAY.equals(normalizedType), errorMsg);

        String elementTypeString = matcher.group(2);
        TypeInformation elementType;
        String normalizedElementType = normalizeType(elementTypeString);
        if (normalizedElementType.startsWith(ROW)) {
            elementType = convertToRow(elementTypeString);
        } else {
            elementType = convertToAtomicType(elementTypeString);
        }

        return Types.OBJECT_ARRAY(elementType);
    }

    /**
     * 目前ROW里只支持基本类型
     * @param rowTypeString
     */
    public static RowTypeInfo convertToRow(String rowTypeString) {
        Matcher matcher = matchCompositeType(rowTypeString);
        final String errorMsg = rowTypeString + "convert to row type error!";
        Preconditions.checkState(matcher.find(), errorMsg);

        String normalizedType = normalizeType(matcher.group(1));
        Preconditions.checkState(ROW.equals(normalizedType), errorMsg);

        String elementTypeStr = matcher.group(2);
        Iterable<String> fieldInfos = splitCompositeTypeField(elementTypeStr);
        Tuple2<TypeInformation[], String[]> info = genFieldInfo(fieldInfos);
        return new RowTypeInfo(info.f0, info.f1);
    }



    private static Tuple2<TypeInformation[], String[]> genFieldInfo(Iterable<String> fieldInfos) {
        ArrayList<TypeInformation> types = Lists.newArrayList();
        ArrayList<String> fieldNames = Lists.newArrayList();

        for (String fieldInfo : fieldInfos) {
            Iterable<String> splitedInfo = splitTypeInfo(fieldInfo);
            ArrayList<String> info = Lists.newArrayList(splitedInfo.iterator());
            Preconditions.checkState(info.size() == 2, "field info must be name with type");
            TypeInformation fieldType = convertToAtomicType(info.get(1));
            types.add(fieldType);
            fieldNames.add(info.get(0));
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
        switch (normalizeType(string)) {
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

    private static Iterable<String> splitTypeInfo(String string) {
        return Splitter
            .on(TYPE_DELIMITER)
            .trimResults()
            .omitEmptyStrings()
            .split(string);
    }

    private static Iterable<String> splitCompositeTypeField(String string) {
        return Splitter
            .on(FIELD_DELIMITER)
            .trimResults()
            .split(string);
    }

    private static String replaceBlank(String s) {
        return s.replaceAll("\\s", " ").trim();
    }

    private static Matcher matchCompositeType(String s) {
        return COMPOSITE_TYPE_PATTERN.matcher(
            replaceBlank(s)
        );
    }

    private static String normalizeType(String s) {
        return s.toUpperCase().trim();
    }
}
