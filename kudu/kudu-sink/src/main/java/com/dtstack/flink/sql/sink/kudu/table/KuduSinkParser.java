package com.dtstack.flink.sql.sink.kudu.table;

import com.dtstack.flink.sql.constant.PluginParamConsts;
import com.dtstack.flink.sql.sink.kudu.KuduOutputFormat;
import com.dtstack.flink.sql.table.AbstractTableParser;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.util.MathUtil;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Map;

import static com.dtstack.flink.sql.table.AbstractTableInfo.PARALLELISM_KEY;

public class KuduSinkParser extends AbstractTableParser {

    public static final String KUDU_MASTERS = "kuduMasters";

    public static final String TABLE_NAME = "tableName";

    public static final String WRITE_MODE = "writeMode";

    public static final String WORKER_COUNT = "workerCount";

    public static final String OPERATION_TIMEOUT_MS = "defaultOperationTimeoutMs";

    public static final String SOCKET_READ_TIMEOUT_MS = "defaultSocketReadTimeoutMs";

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
        kuduTableInfo.setDefaultSocketReadTimeoutMs(MathUtil.getIntegerVal(props.get(SOCKET_READ_TIMEOUT_MS.toLowerCase())));

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
            case "upsert":
                return KuduOutputFormat.WriteMode.UPSERT;
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
