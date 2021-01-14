package com.dtstack.flink.sql.sink.kudu.table;

import com.dtstack.flink.sql.krb.KerberosTable;
import com.dtstack.flink.sql.sink.kudu.KuduOutputFormat;
import com.dtstack.flink.sql.table.AbstractTargetTableInfo;
import com.google.common.base.Preconditions;

public class KuduTableInfo extends AbstractTargetTableInfo implements KerberosTable {

    private static final String CURR_TYPE = "kudu";

    private String kuduMasters;

    private String tableName;

    private KuduOutputFormat.WriteMode writeMode;

    private Integer workerCount;

    private Integer defaultOperationTimeoutMs;

    private Integer defaultSocketReadTimeoutMs;

    /**
     * kerberos
     */
    private String principal;
    private String keytab;
    private String krb5conf;
    boolean enableKrb;
    /**
     * batchSize
     */
    private Integer batchSize;
    private Integer batchWaitInterval;
    /**
     * kudu session flush mode
     */
    private String flushMode;

    public KuduTableInfo() {
        setType(CURR_TYPE);
    }

    public String getKuduMasters() {
        return kuduMasters;
    }

    public void setKuduMasters(String kuduMasters) {
        this.kuduMasters = kuduMasters;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public KuduOutputFormat.WriteMode getWriteMode() {
        return writeMode;
    }

    public void setWriteMode(KuduOutputFormat.WriteMode writeMode) {
        this.writeMode = writeMode;
    }

    public Integer getWorkerCount() {
        return workerCount;
    }

    public void setWorkerCount(Integer workerCount) {
        this.workerCount = workerCount;
    }

    public Integer getDefaultOperationTimeoutMs() {
        return defaultOperationTimeoutMs;
    }

    public void setDefaultOperationTimeoutMs(Integer defaultOperationTimeoutMs) {
        this.defaultOperationTimeoutMs = defaultOperationTimeoutMs;
    }

    public Integer getDefaultSocketReadTimeoutMs() {
        return defaultSocketReadTimeoutMs;
    }

    public void setDefaultSocketReadTimeoutMs(Integer defaultSocketReadTimeoutMs) {
        this.defaultSocketReadTimeoutMs = defaultSocketReadTimeoutMs;
    }

    @Override
    public boolean check() {
        Preconditions.checkNotNull(kuduMasters, "kudu field of kuduMasters is required");
        Preconditions.checkNotNull(tableName, "kudu field of tableName is required");
        return true;
    }

    @Override
    public String getType() {
        return super.getType().toLowerCase();
    }

    @Override
    public String getPrincipal() {
        return principal;
    }

    @Override
    public void setPrincipal(String principal) {
        this.principal = principal;
    }

    @Override
    public String getKeytab() {
        return keytab;
    }

    @Override
    public void setKeytab(String keytab) {
        this.keytab = keytab;
    }

    @Override
    public String getKrb5conf() {
        return krb5conf;
    }

    @Override
    public void setKrb5conf(String krb5conf) {
        this.krb5conf = krb5conf;
    }

    @Override
    public boolean isEnableKrb() {
        return enableKrb;
    }

    @Override
    public void setEnableKrb(boolean enableKrb) {
        this.enableKrb = enableKrb;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
    }

    public Integer getBatchWaitInterval() {
        return batchWaitInterval;
    }

    public void setBatchWaitInterval(Integer batchWaitInterval) {
        this.batchWaitInterval = batchWaitInterval;
    }

    public String getFlushMode() {
        return flushMode;
    }

    public void setFlushMode(String flushMode) {
        this.flushMode = flushMode;
    }

    public enum KuduFlushMode {
        AUTO_FLUSH_SYNC,
        AUTO_FLUSH_BACKGROUND,
        MANUAL_FLUSH
    }
}
