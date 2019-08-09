package com.dtstack.flink.sql.sink.kudu.table;

import com.dtstack.flink.sql.sink.kudu.KuduOutputFormat;
import com.dtstack.flink.sql.table.AbsTableParser;
import com.dtstack.flink.sql.table.TableInfo;
import com.dtstack.flink.sql.util.MathUtil;

import java.util.Map;

import static com.dtstack.flink.sql.table.TableInfo.PARALLELISM_KEY;

public class KuduSinkParser extends AbsTableParser {

    public static final String KUDU_MASTERS = "kuduMasters";

    public static final String TABLE_NAME = "tableName";

    public static final String WRITE_MODE = "writeMode";

    public static final String WORKER_COUNT = "workerCount";

    public static final String OPERATION_TIMEOUT_MS = "defaultOperationTimeoutMs";

    public static final String SOCKET_READ_TIMEOUT_MS = "defaultSocketReadTimeoutMs";

    @Override
    public TableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) {
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
}
