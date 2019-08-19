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

        TableSinkTable targetTable = (TableSinkTable) method.invoke(tableEnv, targetTableName);
        String[] fieldNames = targetTable.tableSink().getFieldNames();

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