package com.dtstack.flink.sql.side.table;

import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.util.DataTypeUtils;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.LookupableTableSource;

/**
 * @author: chuixue
 * @create: 2020-09-28 19:36
 * @description:创建维表类的工厂
 **/
public class LookupTableSourceFactory {

    /**
     * LookupTableSource
     *
     * @param sideTableInfo      表信息
     * @param localSqlPluginPath 插件路径
     * @param pluginLoadMode     插件模式
     * @return
     */
    public static LookupableTableSource createLookupTableSource(AbstractSideTableInfo sideTableInfo, String localSqlPluginPath, String pluginLoadMode) {
        LookupTableSource lookupTableSource = LookupTableSource
                .builder()
                .setAbstractSideTableInfo(sideTableInfo)
                .setSchema(getTableSchema(sideTableInfo))
                .setLocalSqlPluginPath(localSqlPluginPath)
                .setPluginLoadMode(pluginLoadMode)
                .build();
        return lookupTableSource;
    }

    /**
     * 获取tableSource的schema
     *
     * @param sideTableInfo
     * @return
     */
    private static TableSchema getTableSchema(AbstractSideTableInfo sideTableInfo) {
        TableSchema tableSinkSchema = TableSchema
                .builder()
                .fields(
                        sideTableInfo.getFields(),
                        DataTypeUtils.classesToDataTypes(sideTableInfo.getFieldClasses())
                )
                .build();
        return tableSinkSchema;
    }
}
