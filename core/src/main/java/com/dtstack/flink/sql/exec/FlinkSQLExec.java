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

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.delegation.StreamPlanner;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;
import org.apache.flink.table.planner.operations.SqlToOperationConverter;
import org.apache.flink.table.sinks.TableSink;
import scala.Option;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;


/**
 * @description:  mapping by name when insert into sink table
 * @author: maqi
 * @create: 2019/08/15 11:09
 */
public class FlinkSQLExec {

    public static void sqlUpdate(StreamTableEnvironment tableEnv, String stmt) throws Exception {
        StreamTableEnvironmentImpl tableEnvImpl = ((StreamTableEnvironmentImpl) tableEnv);
        StreamPlanner streamPlanner = (StreamPlanner)tableEnvImpl.getPlanner();
        FlinkPlannerImpl flinkPlanner = streamPlanner.createFlinkPlanner();

        RichSqlInsert insert = (RichSqlInsert)flinkPlanner.parse(stmt);
        TableImpl queryResult = extractQueryTableFromInsertCaluse(tableEnvImpl, flinkPlanner, insert);

        String targetTableName = ((SqlIdentifier) ((SqlInsert) insert).getTargetTable()).names.get(0);
        TableSink tableSink = getTableSinkByPlanner(streamPlanner, targetTableName);

        String[] fieldNames = tableSink.getTableSchema().getFieldNames();
        Table newTable = null;
        try {
            newTable = queryResult.select(String.join(",", fieldNames));
        } catch (Exception e) {
            throw new ValidationException(
                    "Field name of query result and registered TableSink "+targetTableName +" do not match.\n" +
                    "Query result schema: " + String.join(",", queryResult.getSchema().getFieldNames()) + "\n" +
                    "TableSink schema: " + String.join(",", fieldNames));
        }
        tableEnv.insertInto(newTable, targetTableName);
    }

    private static TableSink getTableSinkByPlanner(StreamPlanner streamPlanner, String targetTableName)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        Method getTableSink = PlannerBase.class.getDeclaredMethod("getTableSink", List.class);
        getTableSink.setAccessible(true);
        Option tableSinkOption = (Option) getTableSink.invoke(streamPlanner, Arrays.asList(targetTableName));
        return (TableSink) tableSinkOption.get();
    }

    private static TableImpl extractQueryTableFromInsertCaluse(StreamTableEnvironmentImpl tableEnvImpl, FlinkPlannerImpl flinkPlanner, RichSqlInsert insert)
            throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {

        PlannerQueryOperation queryOperation = (PlannerQueryOperation) SqlToOperationConverter.convert(flinkPlanner,
                insert.getSource());
        Method createTableMethod = TableEnvironmentImpl.class.getDeclaredMethod("createTable", QueryOperation.class);
        createTableMethod.setAccessible(true);
        return (TableImpl) createTableMethod.invoke(tableEnvImpl, queryOperation);
    }
}