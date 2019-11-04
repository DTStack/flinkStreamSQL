package com.dtstack.flink.sql.side.kudu.table;

import com.dtstack.flink.sql.table.AbsSideTableParser;
import com.dtstack.flink.sql.table.TableInfo;
import com.dtstack.flink.sql.util.MathUtil;

import java.util.Map;

import static com.dtstack.flink.sql.table.TableInfo.PARALLELISM_KEY;

public class KuduSideParser extends AbsSideTableParser {


    public static final String KUDU_MASTERS = "kuduMasters";

    public static final String TABLE_NAME = "tableName";

    public static final String WORKER_COUNT = "workerCount";

    public static final String OPERATION_TIMEOUT_MS = "defaultOperationTimeoutMs";

    public static final String SOCKET_READ_TIMEOUT_MS = "defaultSocketReadTimeoutMs";

    /**
     * 查询返回的最大字节数
     */
    public static final String BATCH_SIZE_BYTES = "batchSizeBytes";
    /**
     * 查询返回数据条数
     */
    public static final String LIMIT_NUM = "limitNum";

    /**
     * 查询是否容错  查询失败是否扫描第二个副本  默认false  容错
     */
    public static final String IS_FAULT_TO_LERANT = "isFaultTolerant";
    /**
     * 需要过滤的主键
     */
    public static final String PRIMARY_KEY = "primaryKey";
    /**
     * 过滤主键的最小值
     */
    public static final String LOWER_BOUND_PRIMARY_KEY = "lowerBoundPrimaryKey";
    /**
     * 过滤主键的最大值 不包含
     */
    public static final String UPPER_BOUND_PRIMARY_KEY = "upperBoundPrimaryKey";


    @Override
    public TableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) {
        KuduSideTableInfo kuduSideTableInfo = new KuduSideTableInfo();
        kuduSideTableInfo.setName(tableName);
        parseFieldsInfo(fieldsInfo, kuduSideTableInfo);

        parseCacheProp(kuduSideTableInfo, props);

        kuduSideTableInfo.setParallelism(MathUtil.getIntegerVal(props.get(PARALLELISM_KEY.toLowerCase())));
        kuduSideTableInfo.setKuduMasters(MathUtil.getString(props.get(KUDU_MASTERS.toLowerCase())));
        kuduSideTableInfo.setTableName(MathUtil.getString(props.get(TABLE_NAME.toLowerCase())));
        kuduSideTableInfo.setWorkerCount(MathUtil.getIntegerVal(props.get(WORKER_COUNT.toLowerCase())));
        kuduSideTableInfo.setDefaultOperationTimeoutMs(MathUtil.getIntegerVal(props.get(OPERATION_TIMEOUT_MS.toLowerCase())));
        kuduSideTableInfo.setDefaultSocketReadTimeoutMs(MathUtil.getIntegerVal(props.get(SOCKET_READ_TIMEOUT_MS.toLowerCase())));
        kuduSideTableInfo.setBatchSizeBytes(MathUtil.getIntegerVal(props.get(BATCH_SIZE_BYTES.toLowerCase())));
        kuduSideTableInfo.setLimitNum(MathUtil.getLongVal(props.get(LIMIT_NUM.toLowerCase())));
        kuduSideTableInfo.setFaultTolerant(MathUtil.getBoolean(props.get(IS_FAULT_TO_LERANT.toLowerCase())));
        kuduSideTableInfo.setPrimaryKey(MathUtil.getString(props.get(PRIMARY_KEY.toLowerCase())));
        kuduSideTableInfo.setLowerBoundPrimaryKey(MathUtil.getString(props.get(LOWER_BOUND_PRIMARY_KEY.toLowerCase())));
        kuduSideTableInfo.setUpperBoundPrimaryKey(MathUtil.getString(props.get(UPPER_BOUND_PRIMARY_KEY.toLowerCase())));
        return kuduSideTableInfo;

    }
}
