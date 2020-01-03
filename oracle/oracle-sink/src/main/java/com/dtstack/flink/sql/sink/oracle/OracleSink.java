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
package com.dtstack.flink.sql.sink.oracle;

import com.dtstack.flink.sql.sink.IStreamSinkGener;
import com.dtstack.flink.sql.sink.rdb.JDBCOptions;
import com.dtstack.flink.sql.sink.rdb.RdbSink;
import com.dtstack.flink.sql.sink.rdb.dialect.JDBCDialect;
import com.dtstack.flink.sql.sink.rdb.format.JDBCUpsertOutputFormat;

/**
 * Reason:
 * Date: 2018/11/27
 * Company: www.dtstack.com
 *
 * @author maqi
 */
public class OracleSink extends RdbSink implements IStreamSinkGener<RdbSink> {

    public OracleSink() {
        super(new OracleDialect());
    }

    @Override
    public JDBCUpsertOutputFormat getOutputFormat() {
        JDBCOptions jdbcOptions = JDBCOptions.builder()
                .setDBUrl(dbURL).setDialect(jdbcDialect)
                .setUsername(userName).setPassword(password)
                .setTableName(tableName).setScheam(schema).build();

        return JDBCUpsertOutputFormat.builder()
                .setOptions(jdbcOptions)
                .setFieldNames(fieldNames)
                .setFlushMaxSize(batchNum)
                .setFlushIntervalMills(batchWaitInterval)
                .setFieldTypes(sqlTypes)
                .setKeyFields(primaryKeys)
                .setAllReplace(allReplace)
                .setUpdateMode(updateMode).build();
    }


    //    @Override
//    public RetractJDBCOutputFormat getOutputFormat() {
//        return new ExtendOutputFormat();
//    }
//
//    @Override
//    public void buildSql(String scheam, String tableName, List<String> fields) {
//        buildInsertSql(scheam, tableName, fields);
//    }
//
//    private void buildInsertSql(String scheam, String tableName, List<String> fields) {
//        tableName = DtStringUtil.getTableFullPath(scheam, tableName);
//        String columns = fields.stream()
//                .map(this::quoteIdentifier)
//                .collect(Collectors.joining(", "));
//
//        String placeholders = fields.stream()
//                .map(f -> "?")
//                .collect(Collectors.joining(", "));
//        this.sql = "INSERT INTO " + tableName + "(" + columns + ")" + " VALUES (" + placeholders + ")";
//    }
//
//    /**
//     *   use MERGE INTO build oracle replace into sql
//     * @param tableName
//     * @param fieldNames   create table contained  column columns
//     * @param realIndexes  <key: indexName, value: index contains columns >
//     * @param fullField    real columns , query from db
//     * @return
//     */
//    @Override
//    public String buildUpdateSql(String scheam, String tableName, List<String> fieldNames, Map<String, List<String>> realIndexes, List<String> fullField) {
//        tableName = DtStringUtil.getTableFullPath(scheam, tableName);
//
//        StringBuilder sb = new StringBuilder();
//
//        sb.append("MERGE INTO " + tableName + " T1 USING "
//                + "(" + makeValues(fieldNames) + ") T2 ON ("
//                + updateKeySql(realIndexes) + ") ");
//
//
//        String updateSql1 = buildUpdateSqlForAllValue(fieldNames, fullField, "T1", "T2", keyColList(realIndexes));
//        String updateSql = buildUpdateSqlForNotnullValue(fieldNames, fullField, "T1", "T2", keyColList(realIndexes));
//
//        if (StringUtils.isNotEmpty(updateSql)) {
//            sb.append(" WHEN MATCHED THEN UPDATE SET ");
//            sb.append(updateSql);
//        }
//
//        sb.append(" WHEN NOT MATCHED THEN "
//                + "INSERT (" + quoteColumns(fieldNames) + ") VALUES ("
//                + quoteColumns(fieldNames, "T2") + ")");
//
//        return sb.toString();
//    }
//
//
//    public String quoteColumns(List<String> column) {
//        return quoteColumns(column, null);
//    }
//
//    public String quoteColumns(List<String> column, String table) {
//        String prefix = StringUtils.isBlank(table) ? "" : DtStringUtil.addQuoteForStr(table) + ".";
//        List<String> list = new ArrayList<>();
//        for (String col : column) {
//            list.add(prefix + DtStringUtil.addQuoteForStr(col));
//        }
//        return StringUtils.join(list, ",");
//    }
//
//    /**
//     *  extract all distinct index column
//     * @param realIndexes
//     * @return
//     */
//    protected List<String> keyColList(Map<String, List<String>> realIndexes) {
//        List<String> keyCols = new ArrayList<>();
//        for (Map.Entry<String, List<String>> entry : realIndexes.entrySet()) {
//            List<String> list = entry.getValue();
//            for (String col : list) {
//                if (!containsIgnoreCase(keyCols,col)) {
//                    keyCols.add(col);
//                }
//            }
//        }
//        return keyCols;
//    }
//
//    /**
//     *  build update sql , such as UPDATE SET "T1".A="T2".A
//     * @param updateColumn       create table contained  column columns
//     * @param fullColumn   real columns , query from db
//     * @param leftTable    alias
//     * @param rightTable   alias
//     * @param indexCols   index column
//     * @return
//     */
//    public String buildUpdateSqlForAllValue(List<String> updateColumn, List<String> fullColumn, String leftTable, String rightTable, List<String> indexCols) {
//        String prefixLeft = StringUtils.isBlank(leftTable) ? "" : DtStringUtil.addQuoteForStr(leftTable) + ".";
//        String prefixRight = StringUtils.isBlank(rightTable) ? "" : DtStringUtil.addQuoteForStr(rightTable) + ".";
//
//        String sql = fullColumn.stream().filter(col -> {
//            return !(indexCols == null || indexCols.size() == 0 || containsIgnoreCase(indexCols, col));
//        }).map(col -> {
//            String leftCol = prefixLeft + DtStringUtil.addQuoteForStr(col);
//            String rightCol = prefixRight + DtStringUtil.addQuoteForStr(col);
//
//            if (containsIgnoreCase(updateColumn, col)) {
//                return (leftCol + "=" + rightCol);
//            } else {
//                return (leftCol + "=null");
//            }
//        }).collect(Collectors.joining(","));
//
//        return sql;
//    }
//
//    public String buildUpdateSqlForNotnullValue(List<String> updateColumn, List<String> fullColumn, String leftTable, String rightTable, List<String> indexCols) {
//        String prefixLeft = StringUtils.isBlank(leftTable) ? "" : DtStringUtil.addQuoteForStr(leftTable) + ".";
//        String prefixRight = StringUtils.isBlank(rightTable) ? "" : DtStringUtil.addQuoteForStr(rightTable) + ".";
//
//        String sql = fullColumn.stream().filter(col -> {
//            return !(indexCols == null || indexCols.size() == 0 || containsIgnoreCase(indexCols, col));
//        }).map(col -> {
//            String leftCol = prefixLeft + DtStringUtil.addQuoteForStr(col);
//            String rightCol = prefixRight + DtStringUtil.addQuoteForStr(col);
//
//            if (containsIgnoreCase(updateColumn, col)) {
//                return leftCol + "= nvl(" + rightCol + "," + leftCol + ")";
//            }
//            return "";
//        }).collect(Collectors.joining(","));
//
//        return sql;
//    }
//
//
//
//
//    /**
//     *  build connect sql by index column, such as    T1."A"=T2."A"
//     * @param updateKey
//     * @return
//     */
//    public String updateKeySql(Map<String, List<String>> updateKey) {
//        List<String> exprList = new ArrayList<>();
//        for (Map.Entry<String, List<String>> entry : updateKey.entrySet()) {
//            List<String> colList = new ArrayList<>();
//            for (String col : entry.getValue()) {
//                colList.add("T1." + DtStringUtil.addQuoteForStr(col) + "=T2." + DtStringUtil.addQuoteForStr(col));
//            }
//            exprList.add(StringUtils.join(colList, " AND "));
//        }
//        return StringUtils.join(exprList, " OR ");
//    }
//
//    /**
//     *   build select sql , such as (SELECT ? "A",? "B" FROM DUAL)
//     *
//     * @param column   destination column
//     * @return
//     */
//    public String makeValues(List<String> column) {
//        StringBuilder sb = new StringBuilder("SELECT ");
//        for (int i = 0; i < column.size(); ++i) {
//            if (i != 0) {
//                sb.append(",");
//            }
//            sb.append("? " + DtStringUtil.addQuoteForStr(column.get(i)));
//        }
//        sb.append(" FROM DUAL");
//        return sb.toString();
//    }
//
//    public boolean containsIgnoreCase(List<String> l, String s) {
//        Iterator<String> it = l.iterator();
//        while (it.hasNext()) {
//            if (it.next().equalsIgnoreCase(s))
//                return true;
//        }
//        return false;
//    }


}
