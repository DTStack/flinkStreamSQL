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
import com.dtstack.flink.sql.sink.rdb.RdbSink;
import com.dtstack.flink.sql.sink.rdb.format.ExtendOutputFormat;
import com.dtstack.flink.sql.sink.rdb.format.RetractJDBCOutputFormat;
import com.dtstack.flink.sql.util.DtStringUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Reason:
 * Date: 2018/11/27
 * Company: www.dtstack.com
 *
 * @author maqi
 */
public class OracleSink extends RdbSink implements IStreamSinkGener<RdbSink> {
    private static final String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";

    @Override
    public String getDriverName() {
        return ORACLE_DRIVER;
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

        tableName = DtStringUtil.addQuoteForTableName(tableName);
        String sqlTmp = "insert into " + tableName + " (${fields}) values (${placeholder})";

        List<String> adaptFields = Lists.newArrayList();
        fields.forEach(field -> adaptFields.add(DtStringUtil.addQuoteForColumn(field)));

        String fieldsStr = StringUtils.join(adaptFields, ",");
        String placeholder = "";

        for (String fieldName : fields) {
            placeholder += ",?";
        }
        placeholder = placeholder.replaceFirst(",", "");
        sqlTmp = sqlTmp.replace("${fields}", fieldsStr).replace("${placeholder}", placeholder);
        this.sql = sqlTmp;
    }

    /**
     *   use MERGE INTO build oracle replace into sql
     * @param tableName
     * @param fieldNames   create table contained  column columns
     * @param realIndexes  <key: indexName, value: index contains columns >
     * @param fullField    real columns , query from db
     * @return
     */
    @Override
    public String buildUpdateSql(String tableName, List<String> fieldNames, Map<String, List<String>> realIndexes, List<String> fullField) {
        tableName = DtStringUtil.addQuoteForTableName(tableName);
        StringBuilder sb = new StringBuilder();

        sb.append("MERGE INTO " + tableName + " T1 USING "
                + "(" + makeValues(fieldNames) + ") T2 ON ("
                + updateKeySql(realIndexes) + ") ");


        String updateSql = getUpdateSql(fieldNames, fullField, "T1", "T2", keyColList(realIndexes));

        if (StringUtils.isNotEmpty(updateSql)) {
            sb.append(" WHEN MATCHED THEN UPDATE SET ");
            sb.append(updateSql);
        }

        sb.append(" WHEN NOT MATCHED THEN "
                + "INSERT (" + quoteColumns(fieldNames) + ") VALUES ("
                + quoteColumns(fieldNames, "T2") + ")");

        return sb.toString();
    }


    public String quoteColumns(List<String> column) {
        return quoteColumns(column, null);
    }

    public String quoteColumns(List<String> column, String table) {
        String prefix = StringUtils.isBlank(table) ? "" : DtStringUtil.addQuoteForTableName(table) + ".";
        List<String> list = new ArrayList<>();
        for (String col : column) {
            list.add(prefix + DtStringUtil.addQuoteForColumn(col));
        }
        return StringUtils.join(list, ",");
    }

    /**
     *  extract all distinct index column
     * @param realIndexes
     * @return
     */
    protected List<String> keyColList(Map<String, List<String>> realIndexes) {
        List<String> keyCols = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : realIndexes.entrySet()) {
            List<String> list = entry.getValue();
            for (String col : list) {
                if (!containsIgnoreCase(keyCols,col)) {
                    keyCols.add(col);
                }
            }
        }
        return keyCols;
    }

    /**
     *  build update sql , such as UPDATE SET "T1".A="T2".A
     * @param updateColumn       create table contained  column columns
     * @param fullColumn   real columns , query from db
     * @param leftTable    alias
     * @param rightTable   alias
     * @param indexCols   index column
     * @return
     */
    public String getUpdateSql(List<String> updateColumn, List<String> fullColumn, String leftTable, String rightTable, List<String> indexCols) {
        String prefixLeft = StringUtils.isBlank(leftTable) ? "" : DtStringUtil.addQuoteForTableName(leftTable) + ".";
        String prefixRight = StringUtils.isBlank(rightTable) ? "" : DtStringUtil.addQuoteForTableName(rightTable) + ".";
        List<String> list = new ArrayList<>();
        for (String col : fullColumn) {
            // filter index column
            if (indexCols == null || indexCols.size() == 0 || containsIgnoreCase(indexCols,col)) {
                continue;
            }
            if (containsIgnoreCase(updateColumn,col)) {
                list.add(prefixLeft + DtStringUtil.addQuoteForColumn(col) + "=" + prefixRight + DtStringUtil.addQuoteForColumn(col));
            } else {
                list.add(prefixLeft + DtStringUtil.addQuoteForColumn(col) + "=null");
            }
        }
        return StringUtils.join(list, ",");
    }


    /**
     *  build connect sql by index column, such as    T1."A"=T2."A"
     * @param updateKey
     * @return
     */
    public String updateKeySql(Map<String, List<String>> updateKey) {
        List<String> exprList = new ArrayList<>();
        for (Map.Entry<String, List<String>> entry : updateKey.entrySet()) {
            List<String> colList = new ArrayList<>();
            for (String col : entry.getValue()) {
                colList.add("T1." + DtStringUtil.addQuoteForColumn(col) + "=T2." + DtStringUtil.addQuoteForColumn(col));
            }
            exprList.add(StringUtils.join(colList, " AND "));
        }
        return StringUtils.join(exprList, " OR ");
    }

    /**
     *   build select sql , such as (SELECT ? "A",? "B" FROM DUAL)
     *
     * @param column   destination column
     * @return
     */
    public String makeValues(List<String> column) {
        StringBuilder sb = new StringBuilder("SELECT ");
        for (int i = 0; i < column.size(); ++i) {
            if (i != 0) {
                sb.append(",");
            }
            sb.append("? " + DtStringUtil.addQuoteForColumn(column.get(i)));
        }
        sb.append(" FROM DUAL");
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



}
