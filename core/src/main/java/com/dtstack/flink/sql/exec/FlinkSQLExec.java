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
import org.apache.calcite.sql.SqlNode;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.calcite.FlinkPlannerImpl;
import org.apache.flink.table.plan.logical.LogicalRelNode;
import org.apache.flink.table.plan.schema.TableSinkTable;
import org.apache.flink.table.plan.schema.TableSourceSinkTable;
import scala.Option;

import java.lang.reflect.Method;

/**
 * @description:  mapping by name when insert into sink table
 * @author: maqi
 * @create: 2019/08/15 11:09
 */
public class FlinkSQLExec {

    public static void sqlUpdate(StreamTableEnvironment tableEnv, String stmt) throws Exception {

        FlinkPlannerImpl planner = new FlinkPlannerImpl(tableEnv.getFrameworkConfig(), tableEnv.getPlanner(), tableEnv.getTypeFactory());
        SqlNode insert = planner.parse(stmt);

        if (!(insert instanceof SqlInsert)) {
            throw new TableException(
                    "Unsupported SQL query! sqlUpdate() only accepts SQL statements of type INSERT.");
        }
        SqlNode query = ((SqlInsert) insert).getSource();

        SqlNode validatedQuery = planner.validate(query);

        Table queryResult = new Table(tableEnv, new LogicalRelNode(planner.rel(validatedQuery).rel));
        String targetTableName = ((SqlIdentifier) ((SqlInsert) insert).getTargetTable()).names.get(0);

        Method method = TableEnvironment.class.getDeclaredMethod("getTable", String.class);
        method.setAccessible(true);
        Option sinkTab = (Option)method.invoke(tableEnv, targetTableName);

        if (sinkTab.isEmpty()) {
            throw  new ValidationException("Sink table " + targetTableName + "not found in flink");
        }

        TableSourceSinkTable targetTable = (TableSourceSinkTable) sinkTab.get();
        TableSinkTable tableSinkTable = (TableSinkTable)targetTable.tableSinkTable().get();
        String[] fieldNames = tableSinkTable.tableSink().getFieldNames();

        Table newTable = null;
        try {
            newTable = queryResult.select(String.join(",", fieldNames));
        } catch (Exception e) {
            throw new ValidationException(
                    "Field name of query result and registered TableSink "+targetTableName +" do not match.\n" +
                    "Query result schema: " + String.join(",", queryResult.getSchema().getColumnNames()) + "\n" +
                    "TableSink schema: " + String.join(",", fieldNames));
        }

        tableEnv.insertInto(newTable, targetTableName, tableEnv.queryConfig());
    }
}