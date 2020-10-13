package com.dtstack.flink.sql.side.table;

import com.dtstack.flink.sql.enums.ECacheType;
import com.dtstack.flink.sql.enums.EPluginLoadMode;
import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * @author: chuixue
 * @create: 2020-09-29 14:58
 * @description:维表实例(包括全量维表和异步维表)
 **/
public class LookupTableSource implements
        LookupableTableSource<Row> {
    private static final Logger LOG = LoggerFactory.getLogger(LookupTableSource.class);

    private final AbstractSideTableInfo abstractSideTableInfo;
    private String localSqlPluginPath;
    private String pluginLoadMode;
    private final TableSchema schema;

    // 跟维表关联的字段
    private final int[] selectFields;
    private final RowTypeInfo returnType;

    private LookupTableSource(
            AbstractSideTableInfo abstractSideTableInfo
            , TableSchema schema
            , String localSqlPluginPath
            , String pluginLoadMode) {
        this(abstractSideTableInfo
                , schema
                , null
                , localSqlPluginPath
                , pluginLoadMode);
    }

    private LookupTableSource(AbstractSideTableInfo abstractSideTableInfo
            , TableSchema schema
            , int[] selectFields
            , String localSqlPluginPath
            , String pluginLoadMode) {
        this.abstractSideTableInfo = abstractSideTableInfo;
        this.schema = schema;
        this.localSqlPluginPath = localSqlPluginPath;
        this.pluginLoadMode = pluginLoadMode;
        this.selectFields = selectFields;

        final TypeInformation<?>[] schemaTypeInfos = schema.getFieldTypes();
        final String[] schemaFieldNames = schema.getFieldNames();
        if (selectFields != null) {
            TypeInformation<?>[] typeInfos = new TypeInformation[selectFields.length];
            String[] typeNames = new String[selectFields.length];
            for (int i = 0; i < selectFields.length; i++) {
                typeInfos[i] = schemaTypeInfos[selectFields[i]];
                typeNames[i] = schemaFieldNames[selectFields[i]];
            }
            this.returnType = new RowTypeInfo(typeInfos, typeNames);
        } else {
            this.returnType = new RowTypeInfo(schemaTypeInfos, schemaFieldNames);
        }
    }

    /**
     * 全量维表
     *
     * @param lookupKeys 关联字段
     * @return
     */
    @Override
    public TableFunction<Row> getLookupFunction(String[] lookupKeys) {
        TableFunction tableFunction;
        try {
            tableFunction = LookupTableFunctionFactory.createLookupTableFunction(abstractSideTableInfo
                    , localSqlPluginPath
                    , pluginLoadMode
                    , lookupKeys);
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw new RuntimeException(e);
        }
        return tableFunction;
    }

    /**
     * 异步维表
     *
     * @param lookupKeys 关联字段
     * @return
     */
    @Override
    public AsyncTableFunction<Row> getAsyncLookupFunction(String[] lookupKeys) {
        AsyncTableFunction asyncTableFunction;
        try {
            asyncTableFunction = LookupTableFunctionFactory.createLookupAsyncTableFunction(abstractSideTableInfo
                    , localSqlPluginPath
                    , pluginLoadMode
                    , lookupKeys);
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw new RuntimeException(e);
        }
        return asyncTableFunction;
    }

    /**
     * 全量、异步维表选择器
     *
     * @return
     */
    @Override
    public boolean isAsyncEnabled() {
        if (abstractSideTableInfo.getCacheType().equalsIgnoreCase(ECacheType.ALL.name())) {
            return false;
        } else {
            return true;
        }
    }

    @Override
    public TypeInformation<Row> getReturnType() {
        return returnType;
    }

    @Override
    public TableSchema getTableSchema() {
        return schema;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof LookupTableSource) {
            LookupTableSource that = (LookupTableSource) o;
            return Objects.equals(abstractSideTableInfo, that.abstractSideTableInfo) &&
                    Objects.equals(schema, that.schema) &&
                    Objects.equals(localSqlPluginPath, that.localSqlPluginPath) &&
                    Objects.equals(pluginLoadMode, that.pluginLoadMode) &&
                    Arrays.equals(selectFields, that.selectFields) &&
                    Objects.equals(returnType, that.returnType);
        } else {
            return false;
        }
    }

    /**
     * Builder for a {@link LookupTableSource}.
     */
    public static class Builder {

        private AbstractSideTableInfo abstractSideTableInfo;
        private TableSchema schema;
        private String localSqlPluginPath;
        private String pluginLoadMode;

        public Builder setAbstractSideTableInfo(AbstractSideTableInfo abstractSideTableInfo) {
            this.abstractSideTableInfo = abstractSideTableInfo;
            return this;
        }

        public Builder setSchema(TableSchema schema) {
            this.schema = schema;
            return this;
        }

        public Builder setLocalSqlPluginPath(String localSqlPluginPath) {
            this.localSqlPluginPath = localSqlPluginPath;
            return this;
        }

        public Builder setPluginLoadMode(String pluginLoadMode) {
            this.pluginLoadMode = pluginLoadMode;
            return this;
        }

        /**
         * 构建LookupTableSource对象
         *
         * @return
         */
        public LookupTableSource build() {
            checkNotNull(abstractSideTableInfo, "No abstractSideTableInfo supplied.");
            checkNotNull(schema, "No schema supplied.");
            if (!pluginLoadMode.equalsIgnoreCase(EPluginLoadMode.LOCALTEST.name())) {
                checkNotNull(localSqlPluginPath, "No localSqlPluginPath supplied.");
            }
            checkNotNull(pluginLoadMode, "No pluginLoadMode supplied.");
            return new LookupTableSource(abstractSideTableInfo, schema, localSqlPluginPath, pluginLoadMode);
        }
    }
}