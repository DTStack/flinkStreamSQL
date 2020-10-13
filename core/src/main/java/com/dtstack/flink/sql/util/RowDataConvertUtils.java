package com.dtstack.flink.sql.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.Row;

import java.sql.Timestamp;


/**
 * Company: www.dtstack.com
 *
 * @author xuchao
 * @date 2020-05-20
 */
public class RowDataConvertUtils {

    public static RowData convertToRowData(Row row) {
        int length = row.getArity();
        GenericRowData genericRowData = new GenericRowData(length);
        for (int i = 0; i < length; i++) {
            if (row.getField(i) instanceof String) {
                genericRowData.setField(i, BinaryStringData.fromString((String) row.getField(i)));
            } else if (row.getField(i) instanceof Timestamp) {
                TimestampData newTimestamp = TimestampData.fromTimestamp(((Timestamp) row.getField(i)));
                genericRowData.setField(i, newTimestamp);
            } else {
                genericRowData.setField(i, row.getField(i));
            }
        }

        return genericRowData;
    }

    public static LogicalType[] convertTypeInfoToLogicalType(TypeInformation<?>[] fieldTypes) {
        LogicalType[] typeList = new LogicalType[fieldTypes.length];
        for (int i = 0; i < fieldTypes.length; i++) {
            typeList[i] = TypeInfoLogicalTypeConverter.fromTypeInfoToLogicalType(fieldTypes[i]);
        }
        return typeList;
    }

    public static RowData copyRowData(LogicalType[] fieldTypes, RowData rowData) {
        int length = rowData.getArity();
        GenericRowData genericRowData = new GenericRowData(length);
        genericRowData.setRowKind(rowData.getRowKind());
        for (int i = 0; i < length; i++) {
            genericRowData.setField(i, RowData.createFieldGetter(fieldTypes[i], i).getFieldOrNull(rowData));
        }
        return genericRowData;
    }
}
