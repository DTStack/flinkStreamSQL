package com.dtstack.flink.sql.util;

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

    private final static Pattern COMPOUND_TYPE_PATTERN = Pattern.compile("(.+?)<(.+)>");
    private final static String ARRAY = "ARRAY";
    private final static String ROW = "ROW";
    private final static char FIELD_DELIMITER = ',';
    private final static char TYPE_DELIMITER = ' ';

    private final static Pattern ATOMIC_TYPE_PATTERN = Pattern.compile("^\\s*(\\w+)\\s+(\\w+)\\s*,");
    private final static Pattern COMPLEX_TYPE_PATTERN = Pattern.compile("^\\s*(\\w+)\\s+(.+?<.+?>)\\s*,");
    private final static Pattern ATOMIC_TAIL_PATTERN = Pattern.compile("^\\s*(\\w+)\\s+(\\w+)\\s*$");
    private final static Pattern COMPLEX_TAIL_PATTERN = Pattern.compile("^\\s*(\\w+)\\s+(.+?<.+>)\\s*$");

    private DataTypeUtils() {}

    public static TypeInformation convertToCollectionType(String string) {
        Matcher matcher = COMPOUND_TYPE_PATTERN.matcher(string);
        // TODO 现在只支持ARRAY类型后续可以加入 MAP等类型
        if (matcher.find() && ARRAY.equals(matcher.group(1))) {
            return convertToArray(string);
        } else {
            throw new RuntimeException("");
        }
    }

    /**
     * 目前ARRAY里只支持ROW和其他基本类型
     * @param arrayTypeString
     * @return
     */
    public static TypeInformation convertToArray(String arrayTypeString) {
        Matcher matcher = COMPOUND_TYPE_PATTERN.matcher(arrayTypeString);
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
            throw new RuntimeException("convert to array type error!");
        }

    }

    /**
     * 目前ROW里只支持基本类型
     * @param string
     */
    public static RowTypeInfo convertToRow(String string) {
        Matcher matcher = COMPOUND_TYPE_PATTERN.matcher(string);

        if (matcher.find() &&
            ROW.equals(
                matcher.group(1).toUpperCase()
            )
        ) {
            String elementTypeStr = matcher.group(2);
            Iterable<String> typeInfo = splitCompositeTypeFields(elementTypeStr);
            Tuple2<TypeInformation[], String[]> tuple = genFieldInfo(typeInfo);
            return new RowTypeInfo(tuple.f0, tuple.f1);
        } else {
            throw new RuntimeException("");
        }
    }

    private static Iterable<String> splitCompositeTypeFields(String string) {
        return Splitter
            .on(FIELD_DELIMITER)
            .trimResults()
            .split(string);
    }

    private static Tuple2<TypeInformation[], String[]> genFieldInfo(Iterable<String> typeInfo) {
        int fieldsSize = Iterators.size(typeInfo.iterator());
        ArrayList<TypeInformation> types = Lists.newArrayList();
        ArrayList<String> fieldNames = Lists.newArrayList();
        for (String type : typeInfo) {
            Iterable<String> fieldInfo = Splitter
                .on(TYPE_DELIMITER)
                .trimResults()
                .omitEmptyStrings()
                .split(type);
            ArrayList<String> array = Lists.newArrayList(fieldInfo.iterator());
            if (array.size() == 2) {
                TypeInformation fieldType = convertToAtomicType(array.get(1));
                types.add(fieldType);
                fieldNames.add(array.get(0));
            } else {
                throw new RuntimeException();
            }
        }

        TypeInformation[] typesArray = types.toArray(new TypeInformation[types.size()]);
        String[] fieldNamesArray = fieldNames.toArray(new String[fieldNames.size()]);
        return Tuple2.of(typesArray, fieldNamesArray);
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
                throw new RuntimeException("type " + string + "not supported");
        }
    }

    public static ArrayList<String> fieldStmtLexer(String fieldStmts) {

        String stmtStream = fieldStmts;
        ArrayList<String> tokens = new ArrayList<>();
        while (Strings.isNullOrEmpty(stmtStream)) {
            Matcher atomicTypeMatcher = ATOMIC_TYPE_PATTERN.matcher(stmtStream);
            Matcher complexTypeMatcher = COMPLEX_TYPE_PATTERN.matcher(stmtStream);
            Matcher atomicTypeTailMatcher = ATOMIC_TAIL_PATTERN.matcher(stmtStream);
            Matcher complexTypeTailMatcher = COMPLEX_TAIL_PATTERN.matcher(stmtStream);

            String fieldStmt;

            if (atomicTypeMatcher.find()) {
                fieldStmt = atomicTypeMatcher.group(0);
            } else if (complexTypeMatcher.find()) {
                fieldStmt = complexTypeMatcher.group(0);
            } else if (atomicTypeTailMatcher.find()) {
                fieldStmt = atomicTypeTailMatcher.group(0);
            } else if (complexTypeTailMatcher.find()) {
                fieldStmt = complexTypeTailMatcher.group(0);
            } else {
                throw new RuntimeException("field declaration statement error" + fieldStmts);
            }

            tokens.add(fieldStmt);
            stmtStream = stmtStream.substring(fieldStmt.length() + 1);
        }
        return tokens;
    }
}
