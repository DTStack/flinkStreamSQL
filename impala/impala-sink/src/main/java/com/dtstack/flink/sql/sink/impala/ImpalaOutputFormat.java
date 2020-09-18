package com.dtstack.flink.sql.sink.impala;

import com.dtstack.flink.sql.sink.rdb.JDBCOptions;
import com.dtstack.flink.sql.sink.rdb.format.JDBCUpsertOutputFormat;
import com.dtstack.flink.sql.util.KrbUtils;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * @program: flinkStreamSQL
 * @author: wuren
 * @create: 2020/09/11
 **/
public class ImpalaOutputFormat extends JDBCUpsertOutputFormat {
    private String keytabPath;
    private String krb5confPath;
    private String principal;
    private Integer authMech;

    public ImpalaOutputFormat(
        JDBCOptions options,
        String[] fieldNames,
        String[] keyFields,
        String[] partitionFields,
        int[] fieldTypes,
        int flushMaxSize,
        long flushIntervalMills,
        boolean allReplace,
        String updateMode,
        Integer authMech,
        String keytabPath,
        String krb5confPath,
        String principal) {
        super(options,
            fieldNames,
            keyFields,
            partitionFields,
            fieldTypes,
            flushMaxSize,
            flushIntervalMills,
            allReplace,
            updateMode);
        this.authMech = authMech;
        this.keytabPath = keytabPath;
        this.krb5confPath = krb5confPath;
        this.principal = principal;
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        if (authMech == 1) {
            UserGroupInformation ugi = KrbUtils.getUgi(principal, keytabPath, krb5confPath);
            try {
                ugi.doAs(new PrivilegedExceptionAction<Void>() {
                    @Override
                    public Void run() throws IOException {
                        openJdbc();
                        return null;
                    }
                });
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            super.open(taskNumber, numTasks);
        }
    }
    
    public static Builder impalaBuilder() {
        return new Builder();
    }
    
    public static class Builder {
        private Integer authMech;
        private String keytabPath;
        private String krb5confPath;
        private String principal;

        protected JDBCOptions options;
        protected String[] fieldNames;
        protected String[] keyFields;
        protected String[] partitionFields;
        protected int[] fieldTypes;
        protected int flushMaxSize = DEFAULT_FLUSH_MAX_SIZE;
        protected long flushIntervalMills = DEFAULT_FLUSH_INTERVAL_MILLS;
        protected boolean allReplace = DEFAULT_ALLREPLACE_VALUE;
        protected String updateMode;

        /**
         * required, jdbc options.
         */
        public Builder setOptions(JDBCOptions options) {
            this.options = options;
            return this;
        }

        /**
         * required, field names of this jdbc sink.
         */
        public Builder setFieldNames(String[] fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        /**
         * required, upsert unique keys.
         */
        public Builder setKeyFields(List<String> keyFields) {
            this.keyFields = keyFields == null ? null : keyFields.toArray(new String[keyFields.size()]);
            return this;
        }

        /**
         * required, field types of this jdbc sink.
         */
        public Builder setFieldTypes(int[] fieldTypes) {
            this.fieldTypes = fieldTypes;
            return this;
        }

        /**
         * optional, partition Fields
         *
         * @param partitionFields
         * @return
         */
        public Builder setPartitionFields(String[] partitionFields) {
            this.partitionFields = partitionFields;
            return this;
        }

        /**
         * optional, flush max size (includes all append, upsert and delete records),
         * over this number of records, will flush data.
         */
        public Builder setFlushMaxSize(int flushMaxSize) {
            this.flushMaxSize = flushMaxSize;
            return this;
        }

        /**
         * optional, flush interval mills, over this time, asynchronous threads will flush data.
         */
        public Builder setFlushIntervalMills(long flushIntervalMills) {
            this.flushIntervalMills = flushIntervalMills;
            return this;
        }

        public Builder setAllReplace(boolean allReplace) {
            this.allReplace = allReplace;
            return this;
        }

        public Builder setUpdateMode(String updateMode) {
            this.updateMode = updateMode;
            return this;
        }

        public Builder setAuthMech(Integer authMech) {
            this.authMech = authMech;
            return this;
        }
        public Builder setKeytabPath(String keytabPath) {
            this.keytabPath = keytabPath;
            return this;
        }
        public Builder setKrb5confPath(String krb5confPath) {
            this.krb5confPath = krb5confPath;
            return this;
        }
        public Builder setPrincipal(String principal) {
            this.principal = principal;
            return this;
        }
        
        public ImpalaOutputFormat build() {
            checkNotNull(options, "No options supplied.");
            checkNotNull(fieldNames, "No fieldNames supplied.");
            return new ImpalaOutputFormat(
                options,
                fieldNames,
                keyFields,
                partitionFields,
                fieldTypes,
                flushMaxSize,
                flushIntervalMills,
                allReplace,
                updateMode,
                authMech,
                keytabPath,
                krb5confPath,
                principal);
        }
    }
}
