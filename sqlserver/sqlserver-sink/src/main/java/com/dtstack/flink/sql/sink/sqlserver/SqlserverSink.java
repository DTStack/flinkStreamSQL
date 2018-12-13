/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flink.sql.sink.sqlserver;

import com.dtstack.flink.sql.sink.IStreamSinkGener;
import com.dtstack.flink.sql.sink.rdb.RdbSink;
import com.dtstack.flink.sql.sink.rdb.format.ExtendOutputFormat;
import com.dtstack.flink.sql.sink.rdb.format.RetractJDBCOutputFormat;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

/**
 * Reason:
 * Date: 2018/11/27
 * Company: www.dtstack.com
 *
 * @author maqi
 */
public class SqlserverSink extends RdbSink implements IStreamSinkGener<RdbSink> {
    private static final String SQLSERVER_DRIVER = "net.sourceforge.jtds.jdbc.Driver";

    @Override
    public String getDriverName() {
        return SQLSERVER_DRIVER;
    }

    @Override
    public RetractJDBCOutputFormat getOutputFormat() {
        return new ExtendOutputFormat();
    }

    @Override
    public void buildSql(String tableName, List<String> fields) {
        buildInsertSql(tableName, fields);
    }

    private void buildInsertSql(String tableName, List<String> fields) {
        String sqlTmp = "insert into " + tableName + " (${fields}) values (${placeholder})";
        String fieldsStr = StringUtils.join(fields, ",");
        String placeholder = "";

        for (String fieldName : fields) {
            placeholder += ",?";
        }
        placeholder = placeholder.replaceFirst(",", "");
        sqlTmp = sqlTmp.replace("${fields}", fieldsStr).replace("${placeholder}", placeholder);
        this.sql = sqlTmp;
    }

    @Override
    public String buildUpdateSql(String tableName, List<String> fieldNames, Map<String, List<String>> realIndexes, List<String> fullField) {
        return "MERGE INTO " + tableName + " T1 USING "
                + "(" + makeValues(fieldNames) + ") T2 ON ("
                + updateKeySql(realIndexes) + ") WHEN MATCHED THEN UPDATE SET "
                + getUpdateSql(fieldNames, fullField, "T1", "T2", keyColList(realIndexes)) + " WHEN NOT MATCHED THEN "
                + "INSERT (" + quoteColumns(fieldNames) + ") VALUES ("
                + quoteColumns(fieldNames, "T2") + ");";
    }


    public String quoteColumns(List<String> column) {
        return quoteColumns(column, null);
    }

    public String quoteColumns(List<String> column, String table) {
        String prefix = StringUtils.isBlank(table) ? "" : quoteTable(table) + ".";
        List<String> list = new ArrayList<>();
        for (String col : column) {
            list.add(prefix + quoteColumn(col));
        }
        return StringUtils.join(list, ",");
    }

    protected List<String> keyColList(Map<String, List<String>> updateKey) {
        List<String> keyCols = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : updateKey.entrySet()) {
            List<String> list = entry.getValue();
            for (String col : list) {
                if (!containsIgnoreCase(keyCols,col)) {
                    keyCols.add(col);
                }
            }
        }
        return keyCols;
    }

    public String getUpdateSql(List<String> column, List<String> fullColumn, String leftTable, String rightTable, List<String> keyCols) {
        String prefixLeft = StringUtils.isBlank(leftTable) ? "" : quoteTable(leftTable) + ".";
        String prefixRight = StringUtils.isBlank(rightTable) ? "" : quoteTable(rightTable) + ".";
        List<String> list = new ArrayList<>();
        for (String col : fullColumn) {
            if (keyCols == null || keyCols.size() == 0) {
                continue;
            }
            if (fullColumn == null || containsIgnoreCase(column,col)) {
                list.add(prefixLeft + col + "=" + prefixRight + col);
            } else {
                list.add(prefixLeft + col + "=null");
            }
        }
        return StringUtils.join(list, ",");
    }

    public String quoteTable(String table) {
        String[] parts = table.split("\\.");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.length; ++i) {
            if (i != 0) {
                sb.append(".");
            }
            sb.append(getStartQuote() + parts[i] + getEndQuote());
        }
        return sb.toString();
    }


    public String updateKeySql(Map<String, List<String>> updateKey) {
        List<String> exprList = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : updateKey.entrySet()) {
            List<String> colList = new ArrayList<>();
            for (String col : entry.getValue()) {
                colList.add("T1." + quoteColumn(col) + "=T2." + quoteColumn(col));
            }
            exprList.add(StringUtils.join(colList, " AND "));
        }
        return StringUtils.join(exprList, " OR ");
    }


    public String makeValues(List<String> column) {
        StringBuilder sb = new StringBuilder("SELECT ");
        for (int i = 0; i < column.size(); ++i) {
            if (i != 0) {
                sb.append(",");
            }
            sb.append("? " + quoteColumn(column.get(i)));
        }
        return sb.toString();
    }

    public boolean containsIgnoreCase(List<String> l, String s) {
        Iterator<String> it = l.iterator();
        while (it.hasNext()) {
            if (it.next().equalsIgnoreCase(s))
                return true;
        }
        return false;
    }
    public String quoteColumn(String column) {
        return getStartQuote() + column + getEndQuote();
    }

    public String getStartQuote() {
        return "\"";
    }

    public String getEndQuote() {
        return "\"";
    }


}
