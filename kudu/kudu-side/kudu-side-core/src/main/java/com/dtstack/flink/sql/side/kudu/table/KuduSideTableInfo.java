package com.dtstack.flink.sql.side.kudu.table;

import com.dtstack.flink.sql.krb.KerberosTable;
import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.google.common.base.Preconditions;

public class KuduSideTableInfo extends AbstractSideTableInfo implements KerberosTable {

    private static final String CURR_TYPE = "kudu";

    private static final long serialVersionUID = 1085582743577521861L;

    private String kuduMasters;

    private String tableName;

    private Integer workerCount;

    private Integer defaultOperationTimeoutMs;

    private Integer defaultSocketReadTimeoutMs;
    /**
     * 查询返回的最大字节数
     */
    private Integer batchSizeBytes;

    /**
     * 查询返回数据条数
     */
    private Long limitNum;
    /**
     * 查询是否容错  查询失败是否扫描第二个副本  默认false  容错
     */
    private Boolean isFaultTolerant;

    /**
     * 需要过滤的主键
     */
    private String primaryKey;
    /**
     * 过滤主键的最小值
     */
    private String lowerBoundPrimaryKey;
    /**
     * 过滤主键的最大值 不包含
     */
    private String upperBoundPrimaryKey;

    /**
     * kerberos
     */
    private String principal;
    private String keytab;
    private String krb5conf;
    boolean enableKrb;

    public KuduSideTableInfo() {
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

    public Integer getBatchSizeBytes() {
        return batchSizeBytes;
    }

    public void setBatchSizeBytes(Integer batchSizeBytes) {
        this.batchSizeBytes = batchSizeBytes;
    }

    public Long getLimitNum() {
        return limitNum;
    }

    public void setLimitNum(Long limitNum) {
        this.limitNum = limitNum;
    }

    public Boolean getFaultTolerant() {
        return isFaultTolerant;
    }

    public void setFaultTolerant(Boolean faultTolerant) {
        isFaultTolerant = faultTolerant;
    }

    public String getLowerBoundPrimaryKey() {
        return lowerBoundPrimaryKey;
    }

    public void setLowerBoundPrimaryKey(String lowerBoundPrimaryKey) {
        this.lowerBoundPrimaryKey = lowerBoundPrimaryKey;
    }

    public String getUpperBoundPrimaryKey() {
        return upperBoundPrimaryKey;
    }

    public void setUpperBoundPrimaryKey(String upperBoundPrimaryKey) {
        this.upperBoundPrimaryKey = upperBoundPrimaryKey;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
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

}
