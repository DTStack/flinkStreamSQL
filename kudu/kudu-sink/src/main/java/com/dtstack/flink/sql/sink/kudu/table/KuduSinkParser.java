package com.dtstack.flink.sql.sink.kudu.table;

import com.dtstack.flink.sql.constant.PluginParamConsts;
import com.dtstack.flink.sql.sink.kudu.KuduOutputFormat;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.table.AbstractTableParser;
import com.dtstack.flink.sql.util.MathUtil;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Objects;

import static com.dtstack.flink.sql.table.AbstractTableInfo.PARALLELISM_KEY;

public class KuduSinkParser extends AbstractTableParser {

    public static final String KUDU_MASTERS = "kuduMasters";

    public static final String TABLE_NAME = "tableName";

    public static final String WRITE_MODE = "writeMode";

    public static final String WORKER_COUNT = "workerCount";

    public static final String OPERATION_TIMEOUT_MS = "defaultOperationTimeoutMs";

    public static final String BATCH_SIZE_KEY = "batchSize";

    public static final Integer DEFAULT_BATCH_SIZE = 1000;

    public static final String BATCH_WAIT_INTERVAL_KEY = "batchWaitInterval";

    public static final String BUFFER_MAX_KEY = "mutationBufferMaxOps";

    public static final Integer DEFAULT_BATCH_WAIT_INTERVAL = 60 * 1000;

    public static final String SESSION_FLUSH_MODE_KEY = "flushMode";

    @Override
    public AbstractTableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) {
        KuduTableInfo kuduTableInfo = new KuduTableInfo();
        kuduTableInfo.setName(tableName);
        parseFieldsInfo(fieldsInfo, kuduTableInfo);

        kuduTableInfo.setParallelism(MathUtil.getIntegerVal(props.get(PARALLELISM_KEY.toLowerCase())));
        kuduTableInfo.setKuduMasters(MathUtil.getString(props.get(KUDU_MASTERS.toLowerCase())));
        kuduTableInfo.setTableName(MathUtil.getString(props.get(TABLE_NAME.toLowerCase())));
        kuduTableInfo.setWriteMode(transWriteMode(MathUtil.getString(props.get(WRITE_MODE.toLowerCase()))));
        kuduTableInfo.setWorkerCount(MathUtil.getIntegerVal(props.get(WORKER_COUNT.toLowerCase())));
        kuduTableInfo.setDefaultOperationTimeoutMs(MathUtil.getIntegerVal(props.get(OPERATION_TIMEOUT_MS.toLowerCase())));
        kuduTableInfo.setBatchSize(MathUtil.getIntegerVal(props.getOrDefault(BATCH_SIZE_KEY.toLowerCase(), DEFAULT_BATCH_SIZE)));
        kuduTableInfo.setBatchWaitInterval(MathUtil.getIntegerVal(props.getOrDefault(BATCH_WAIT_INTERVAL_KEY.toLowerCase(), DEFAULT_BATCH_WAIT_INTERVAL)));
        kuduTableInfo.setMutationBufferMaxOps(MathUtil.getIntegerVal(props.get(BUFFER_MAX_KEY.toLowerCase())));

        if (Objects.isNull(props.get(SESSION_FLUSH_MODE_KEY.toLowerCase()))) {
            if (kuduTableInfo.getBatchSize() > 1) {
                kuduTableInfo.setFlushMode(KuduTableInfo.KuduFlushMode.MANUAL_FLUSH.name());
            } else {
                kuduTableInfo.setFlushMode(KuduTableInfo.KuduFlushMode.AUTO_FLUSH_SYNC.name());
            }
        } else {
            kuduTableInfo.setFlushMode(MathUtil.getString(props.get(SESSION_FLUSH_MODE_KEY.toLowerCase())));
        }

        kuduTableInfo.setPrincipal(
                MathUtil.getString(props.get(PluginParamConsts.PRINCIPAL))
        );
        kuduTableInfo.setKeytab(
                MathUtil.getString(props.get(PluginParamConsts.KEYTAB))
        );
        kuduTableInfo.setKrb5conf(
                MathUtil.getString(props.get(PluginParamConsts.KRB5_CONF))
        );
        kuduTableInfo.judgeKrbEnable();

        return kuduTableInfo;
    }

    private KuduOutputFormat.WriteMode transWriteMode(String writeMode) {
        switch (writeMode) {
            case "insert":
                return KuduOutputFormat.WriteMode.INSERT;
            case "update":
                return KuduOutputFormat.WriteMode.UPDATE;
            default:
                return KuduOutputFormat.WriteMode.UPSERT;
        }
    }

    @Override
    public Class dbTypeConvertToJavaType(String fieldType) {
        switch (fieldType.toLowerCase()) {
            case "boolean":
            case "bool":
                return Boolean.class;
            case "int8":
                return Byte.class;
            case "int16":
                return Short.class;
            case "int":
            case "int32":
                return Integer.class;
            case "long":
            case "int64":
                return Long.class;
            case "varchar":
            case "string":
                return String.class;
            case "float":
                return Float.class;
            case "double":
                return Double.class;
            case "date":
                return Date.class;
            case "unixtime_micros":
            case "timestamp":
                return Timestamp.class;
            case "decimal":
                return BigDecimal.class;
            case "binary":
                return byte[].class;
            default:
        }

        throw new RuntimeException("不支持 " + fieldType + " 类型");
    }
}
