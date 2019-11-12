package com.dtstack.flink.sql.sink.kudu.table;

import com.dtstack.flink.sql.sink.kudu.KuduOutputFormat;
import com.dtstack.flink.sql.table.TargetTableInfo;
import org.apache.flink.calcite.shaded.com.google.common.base.Preconditions;

public class KuduTableInfo extends TargetTableInfo {

    private static final String CURR_TYPE = "kudu";

    private String kuduMasters;

    private String tableName;

    private KuduOutputFormat.WriteMode writeMode;

    private Integer workerCount;

    private Integer defaultOperationTimeoutMs;

    private Integer defaultSocketReadTimeoutMs;

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
}
