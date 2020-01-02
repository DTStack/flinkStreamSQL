package com.dtstack.flink.sql.sink.db;

import com.dtstack.flink.sql.sink.rdb.RdbSink;

import java.util.List;
import java.util.Map;

public class DbSink extends RdbSink {

    private static final String DB2_DRIVER = "com.ibm.db2.jcc.DB2Driver";

    public DbSink() {
    }

    @Override
    public void buildSql(String schema, String tableName, List<String> fields) {
        buildInsertSql(tableName, fields);
    }

    private void buildInsertSql(String tableName, List<String> fields) {
        String sqlTmp = "insert into " +  tableName + " (${fields}) values (${placeholder})";
        String fieldsStr = "";
        String placeholder = "";

        for (String fieldName : fields) {
            fieldsStr += "," + fieldName;
            placeholder += ",?";
        }

        fieldsStr = fieldsStr.replaceFirst(",", "");
        placeholder = placeholder.replaceFirst(",", "");

        sqlTmp = sqlTmp.replace("${fields}", fieldsStr).replace("${placeholder}", placeholder);
        this.sql = sqlTmp;
    }

    @Override
    public String buildUpdateSql(String schema, String tableName, List<String> fieldNames, Map<String, List<String>> realIndexes, List<String> fullField) {
        return null;
    }

    @Override
    public String getDriverName() {
        return DB2_DRIVER;
    }

    @Override
    public RetractJDBCOutputFormat getOutputFormat() {
        return new RetractJDBCOutputFormat();
    }
}
