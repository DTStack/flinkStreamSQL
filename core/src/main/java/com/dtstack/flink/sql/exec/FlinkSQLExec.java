/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flink.sql.exec;

import com.dtstack.flink.sql.util.SqlFormatterUtil;
import com.google.common.collect.Maps;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.delegation.StreamPlanner;
import org.apache.flink.table.planner.operations.SqlToOperationConverter;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.sinks.TableSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;


/**
 * @description:  mapping by name when insert into sink table
 * @author: maqi
 * @create: 2019/08/15 11:09
 */
public class FlinkSQLExec {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkSQLExec.class);
    // create view必须使用别名,flink bug
    private static final String CREATE_VIEW_ERR_INFO = "SQL parse failed. Encountered \"FOR\"";
    private static final String CREATE_VIEW_ERR_SQL = "CREATE VIEW view_out AS select id, name FROM source LEFT JOIN side FOR SYSTEM_TIME AS OF source.PROCTIME ON source.id = side.sid;";
    private static final String CREATE_VIEW_RIGHT_SQL = "CREATE VIEW view_out AS select u.id, u.name FROM source u LEFT JOIN side FOR SYSTEM_TIME AS OF u.PROCTIME AS s ON u.id = s.sid;";

    public static void sqlUpdate(StreamTableEnvironment tableEnv, String stmt) throws Exception {
        StreamTableEnvironmentImpl tableEnvImpl = ((StreamTableEnvironmentImpl) tableEnv);
        StreamPlanner streamPlanner = (StreamPlanner)tableEnvImpl.getPlanner();
        FlinkPlannerImpl flinkPlanner = streamPlanner.createFlinkPlanner();

        RichSqlInsert insert = (RichSqlInsert) flinkPlanner.validate(flinkPlanner.parser().parse(stmt));
        TableImpl queryResult;
        try {
            queryResult = extractQueryTableFromInsertCaluse(tableEnvImpl, flinkPlanner, insert);
        } catch (SqlParserException e) {
            if (e.getMessage().contains(CREATE_VIEW_ERR_INFO)) {
                throw new RuntimeException(buildErrorMsg());
            } else {
                throw new RuntimeException(e.getMessage());
            }
        }

        String targetTableName = ((SqlIdentifier) insert.getTargetTable()).names.get(0);
        TableSink tableSink = getTableSinkByPlanner(streamPlanner, targetTableName);

        String[] sinkFieldNames = tableSink.getTableSchema().getFieldNames();
        String[] queryFieldNames = queryResult.getSchema().getFieldNames();

        if (sinkFieldNames.length != queryFieldNames.length) {
            throw new ValidationException(
                    "Field name of query result and registered TableSink " + targetTableName + " do not match.\n" +
                            "Query result schema: " + String.join(",", queryFieldNames) + "\n" +
                            "TableSink schema: " + String.join(",", sinkFieldNames));
        }


        Table newTable;
        try {
            newTable = queryResult.select(String.join(",", sinkFieldNames));
        } catch (Exception e) {
            throw new ValidationException(
                    "Field name of query result and registered TableSink "+targetTableName +" do not match.\n" +
                    "Query result schema: " + String.join(",", queryFieldNames) + "\n" +
                    "TableSink schema: " + String.join(",", sinkFieldNames));
        }

        try {
            tableEnv.insertInto(targetTableName, newTable);
        } catch (Exception e) {
            LOG.warn("Field name case of query result and registered TableSink do not match. ", e);
            newTable = queryResult.select(String.join(",", ignoreCase(queryFieldNames, sinkFieldNames)));
            tableEnv.insertInto(targetTableName, newTable);
        }

    }

    private static TableSink getTableSinkByPlanner(StreamPlanner streamPlanner, String targetTableName)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Method getTableSink = PlannerBase.class.getDeclaredMethod("getTableSink", ObjectIdentifier.class, Map.class);
        getTableSink.setAccessible(true);
        ObjectIdentifier objectIdentifier = ObjectIdentifier.of(streamPlanner.catalogManager().getCurrentCatalog(), streamPlanner.catalogManager().getCurrentDatabase(), targetTableName);
        Map<String, String> dynamicOptions = Maps.newHashMap();
        Option tableSinkOption = (Option) getTableSink.invoke(streamPlanner, objectIdentifier, dynamicOptions);
        return (TableSink) ((Tuple2) tableSinkOption.get())._2;
    }

    private static TableImpl extractQueryTableFromInsertCaluse(StreamTableEnvironmentImpl tableEnvImpl, FlinkPlannerImpl flinkPlanner, RichSqlInsert insert)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        StreamPlanner streamPlanner = (StreamPlanner) tableEnvImpl.getPlanner();
        Operation queryOperation = SqlToOperationConverter.convert(flinkPlanner, streamPlanner.catalogManager(), insert.getSource()).get();
        Method createTableMethod = TableEnvironmentImpl.class.getDeclaredMethod("createTable", QueryOperation.class);
        createTableMethod.setAccessible(true);
        return (TableImpl) createTableMethod.invoke(tableEnvImpl, queryOperation);
    }

    private static String[] ignoreCase(String[] queryFieldNames, String[] sinkFieldNames) {
        String[] newFieldNames = sinkFieldNames;
        for (int i = 0; i < newFieldNames.length; i++) {
            for (String queryFieldName : queryFieldNames) {
                if (newFieldNames[i].equalsIgnoreCase(queryFieldName)) {
                    newFieldNames[i] = queryFieldName;
                    break;
                }
            }
        }
        return newFieldNames;
    }

    /**
     * create view 语法错误提示
     * @return
     */
    private static String buildErrorMsg() {
        return "\n"
                + SqlFormatterUtil.format(CREATE_VIEW_ERR_SQL)
                + "\n========== not support ,please use dimension table alias ==========\n"
                + SqlFormatterUtil.format(CREATE_VIEW_RIGHT_SQL);
    }
}