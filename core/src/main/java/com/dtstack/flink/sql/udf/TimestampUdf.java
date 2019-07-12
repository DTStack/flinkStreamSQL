package com.dtstack.flink.sql.udf;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

import java.sql.Timestamp;

public class TimestampUdf extends ScalarFunction {
    @Override
    public void open(FunctionContext context) {
    }
    public static Timestamp eval(String timestamp) {
        if (timestamp.length() == 13){
            return new Timestamp(Long.parseLong(timestamp));
        }else if (timestamp.length() == 10){
            return new Timestamp(Long.parseLong(timestamp)*1000);
        } else{
            return Timestamp.valueOf(timestamp);
        }
    }
    @Override
    public void close() {
    }
}
