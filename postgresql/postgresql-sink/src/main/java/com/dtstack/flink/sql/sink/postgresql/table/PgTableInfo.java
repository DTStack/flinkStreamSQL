package com.dtstack.flink.sql.sink.postgresql.table;

import com.dtstack.flink.sql.table.TargetTableInfo;

/**
 * Date: 2019-08-22
 * Company: mmg
 *
 * @author tcm
 */
public class PgTableInfo extends TargetTableInfo {

    private String url;

    private String tableName;

    private String userName;

    private String password;

    private Integer batchSize;

    private Long batchWaitInterval;

    private String bufferSize;

    private String flushIntervalMs;

    private static final String CURR_TYPE = "postgresql";
    //支持Cockroach数据库true/false
    private boolean isCockroach;
    private String keyField;

    public PgTableInfo() {
        setType(CURR_TYPE);
    }

    public boolean isCockroach() {
        return isCockroach;
    }

    public void setCockroach(boolean cockroach) {
        isCockroach = cockroach;
    }

    public String getKeyField() {
        return keyField;
    }

    public void setKeyField(String keyField) {
        this.keyField = keyField;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
    }

    public Long getBatchWaitInterval() {
        return batchWaitInterval;
    }

    public void setBatchWaitInterval(Long batchWaitInterval) {
        this.batchWaitInterval = batchWaitInterval;
    }

    public String getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(String bufferSize) {
        this.bufferSize = bufferSize;
    }

    public String getFlushIntervalMs() {
        return flushIntervalMs;
    }

    public void setFlushIntervalMs(String flushIntervalMs) {
        this.flushIntervalMs = flushIntervalMs;
    }

    @Override
    public boolean check() {
        //Preconditions.checkArgument(isCockroach == false && StringUtils.isEmpty(keyField), "");
        return true;
    }
}
