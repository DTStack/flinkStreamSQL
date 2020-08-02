package com.dtstack.flink.sql.util;

import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.dataformat.SqlTimestamp;
import org.apache.flink.types.Row;

import java.sql.Timestamp;


/**
 * Company: www.dtstack.com
 *
 * @author xuchao
 * @date 2020-05-20
 */
public class RowDataConvert {

    public static BaseRow convertToBaseRow(Row row){
        int length = row.getArity();
        GenericRow genericRow = new GenericRow(length);
        for(int i=0; i<length; i++){
            if(row.getField(i) instanceof String){
                genericRow.setField(i, BinaryString.fromString((String)row.getField(i)));
            } else if(row.getField(i) instanceof Timestamp){
                SqlTimestamp newTimestamp = SqlTimestamp.fromTimestamp(((Timestamp)row.getField(i)));
                genericRow.setField(i, newTimestamp);
            }else{
                genericRow.setField(i, row.getField(i));
            }
        }

        return genericRow;
    }
}
