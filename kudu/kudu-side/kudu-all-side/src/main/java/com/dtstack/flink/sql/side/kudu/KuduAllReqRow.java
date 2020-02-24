package com.dtstack.flink.sql.side.kudu;

import com.dtstack.flink.sql.side.AllReqRow;
import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.PredicateInfo;
import com.dtstack.flink.sql.side.SideTableInfo;
import com.dtstack.flink.sql.side.kudu.table.KuduSideTableInfo;
import com.dtstack.flink.sql.side.kudu.utils.KuduUtil;
import org.apache.calcite.sql.JoinType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class KuduAllReqRow extends AllReqRow {

    private static final long serialVersionUID = 6051774809356082219L;

    private static final Logger LOG = LoggerFactory.getLogger(KuduAllReqRow.class);
    /**
     * 获取连接的尝试次数
     */
    private static final int CONN_RETRY_NUM = 3;
    /**
     * 缓存条数
     */
    private static final Long FETCH_SIZE = 1000L;

    private KuduClient client;

    private KuduTable table;


    private AtomicReference<Map<String, List<Map<String, Object>>>> cacheRef = new AtomicReference<>();

    public KuduAllReqRow(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) {
        super(new KuduAllSideInfo(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo));
    }


    @Override
    public Row fillData(Row input, Object sideInput) {
        Map<String, Object> cacheInfo = (Map<String, Object>) sideInput;
        Row row = new Row(sideInfo.getOutFieldInfoList().size());
        for (Map.Entry<Integer, Integer> entry : sideInfo.getInFieldIndex().entrySet()) {
            Object obj = input.getField(entry.getValue());
            boolean isTimeIndicatorTypeInfo = TimeIndicatorTypeInfo.class.isAssignableFrom(sideInfo.getRowTypeInfo().getTypeAt(entry.getValue()).getClass());

            //Type information for indicating event or processing time. However, it behaves like a regular SQL timestamp but is serialized as Long.
            if (obj instanceof Timestamp && isTimeIndicatorTypeInfo) {
                obj = ((Timestamp) obj).getTime();
            }
            row.setField(entry.getKey(), obj);
        }

        for (Map.Entry<Integer, String> entry : sideInfo.getSideFieldNameIndex().entrySet()) {
            if (cacheInfo == null) {
                row.setField(entry.getKey(), null);
            } else {
                row.setField(entry.getKey(), cacheInfo.get(entry.getValue()));
            }
        }

        return row;
    }

    @Override
    protected void initCache() throws SQLException {
        Map<String, List<Map<String, Object>>> newCache = Maps.newConcurrentMap();
        cacheRef.set(newCache);
        loadData(newCache);
    }


    @Override
    protected void reloadCache() {
        //reload cacheRef and replace to old cacheRef
        Map<String, List<Map<String, Object>>> newCache = Maps.newConcurrentMap();
        loadData(newCache);

        cacheRef.set(newCache);
        LOG.info("----- kudu all cacheRef reload end:{}", Calendar.getInstance());
    }


    @Override
    public void flatMap(CRow input, Collector<CRow> out) throws Exception {
        List<Object> inputParams = Lists.newArrayList();
        for (Integer conValIndex : sideInfo.getEqualValIndex()) {
            Object equalObj = input.row().getField(conValIndex);
            if (equalObj == null) {
                out.collect(null);
            }
            inputParams.add(equalObj);
        }

        String key = buildKey(inputParams);
        List<Map<String, Object>> cacheList = cacheRef.get().get(key);
        if (CollectionUtils.isEmpty(cacheList)) {
            if (sideInfo.getJoinType() == JoinType.LEFT) {
                Row row = fillData(input.row(), null);
                out.collect(new CRow(row, input.change()));
            }
            return;
        }

        for (Map<String, Object> one : cacheList) {
            out.collect(new CRow(fillData(input.row(), one), input.change()));
        }
    }

    private void loadData(Map<String, List<Map<String, Object>>> tmpCache) {
        KuduSideTableInfo tableInfo = (KuduSideTableInfo) sideInfo.getSideTableInfo();
        KuduScanner scanner = null;
        try {
            for (int i = 0; i < CONN_RETRY_NUM; i++) {
                try {
                    scanner = getConn(tableInfo);
                    break;
                } catch (Exception e) {
                    if (i == CONN_RETRY_NUM - 1) {
                        throw new RuntimeException("", e);
                    }
                    try {
                        String connInfo = "kuduMasters:" + tableInfo.getKuduMasters() + ";tableName:" + tableInfo.getTableName();
                        LOG.warn("get conn fail, wait for 5 sec and try again, connInfo:" + connInfo);
                        Thread.sleep(5 * 1000);
                    } catch (InterruptedException e1) {
                        LOG.error("",e1);
                    }
                }
            }
            //load data from table
            assert scanner != null;
            String[] sideFieldNames = sideInfo.getSideSelectFields().split(",");


            while (scanner.hasMoreRows()) {
                RowResultIterator results = scanner.nextRows();
                while (results.hasNext()) {
                    RowResult result = results.next();
                    Map<String, Object> oneRow = Maps.newHashMap();
                    for (String sideFieldName1 : sideFieldNames) {
                        String sideFieldName = sideFieldName1.trim();
                        ColumnSchema columnSchema = table.getSchema().getColumn(sideFieldName);
                        if (null != columnSchema) {
                            KuduUtil.setMapValue(columnSchema.getType(), oneRow, sideFieldName, result);
                        }
                    }
                    String cacheKey = buildKey(oneRow, sideInfo.getEqualFieldList());
                    List<Map<String, Object>> list = tmpCache.computeIfAbsent(cacheKey, key -> Lists.newArrayList());
                    list.add(oneRow);
                }
            }

        } catch (Exception e) {
            LOG.error("", e);
        } finally {
            if (null != scanner) {
                try {
                    scanner.close();
                } catch (KuduException e) {
                    LOG.error("Error while closing scanner.", e);
                }
            }
        }


    }

    private String buildKey(List<Object> equalValList) {
        StringBuilder sb = new StringBuilder("");
        for (Object equalVal : equalValList) {
            sb.append(equalVal).append("_");
        }

        return sb.toString();
    }

    private String buildKey(Map<String, Object> val, List<String> equalFieldList) {
        StringBuilder sb = new StringBuilder("");
        for (String equalField : equalFieldList) {
            sb.append(val.get(equalField)).append("_");
        }
        return sb.toString();
    }

    private KuduScanner getConn(KuduSideTableInfo tableInfo) {
        try {
            if (client == null) {
                String kuduMasters = tableInfo.getKuduMasters();
                String tableName = tableInfo.getTableName();
                Integer workerCount = tableInfo.getWorkerCount();
                Integer defaultSocketReadTimeoutMs = tableInfo.getDefaultSocketReadTimeoutMs();
                Integer defaultOperationTimeoutMs = tableInfo.getDefaultOperationTimeoutMs();

                Preconditions.checkNotNull(kuduMasters, "kuduMasters could not be null");

                KuduClient.KuduClientBuilder kuduClientBuilder = new KuduClient.KuduClientBuilder(kuduMasters);
                if (null != workerCount) {
                    kuduClientBuilder.workerCount(workerCount);
                }

                if (null != defaultOperationTimeoutMs) {
                    kuduClientBuilder.defaultOperationTimeoutMs(defaultOperationTimeoutMs);
                }
                client = kuduClientBuilder.build();

                if (!client.tableExists(tableName)) {
                    throw new IllegalArgumentException("Table Open Failed , please check table exists");
                }
                table = client.openTable(tableName);
            }
            Schema schema = table.getSchema();
            KuduScanner.KuduScannerBuilder tokenBuilder = client.newScannerBuilder(table);
            return buildScanner(tokenBuilder, schema, tableInfo);
        } catch (Exception e) {
            LOG.error("connect kudu is error:" + e.getMessage());
            throw new RuntimeException(e);
        }
    }


    /**
     * @param builder   创建AsyncKuduScanner对象
     * @param schema    kudu中表约束
     * @param tableInfo AsyncKuduScanner的配置信息
     * @return
     */
    private KuduScanner buildScanner(KuduScanner.KuduScannerBuilder builder, Schema schema, KuduSideTableInfo tableInfo) {
        Integer batchSizeBytes = tableInfo.getBatchSizeBytes();
        Long limitNum = tableInfo.getLimitNum();
        Boolean isFaultTolerant = tableInfo.getFaultTolerant();
        //查询需要的字段
        String[] sideFieldNames = sideInfo.getSideSelectFields().split(",");
        //主键过滤条件 主键最小值
        String lowerBoundPrimaryKey = tableInfo.getLowerBoundPrimaryKey();
        //主键过滤条件 主键最大值
        String upperBoundPrimaryKey = tableInfo.getUpperBoundPrimaryKey();
        //主键字段
        String primaryKeys = tableInfo.getPrimaryKey();
        if (null == limitNum || limitNum <= 0) {
            builder.limit(FETCH_SIZE);
        } else {
            builder.limit(limitNum);
        }
        if (null != batchSizeBytes) {
            builder.batchSizeBytes(batchSizeBytes);
        }
        if (null != isFaultTolerant) {
            builder.setFaultTolerant(isFaultTolerant);
        }
        //  填充谓词信息
        List<PredicateInfo> predicateInfoes = sideInfo.getSideTableInfo().getPredicateInfoes();
        if (predicateInfoes.size() > 0) {
            predicateInfoes.stream().map(info -> {
                KuduPredicate kuduPredicate = KuduUtil.buildKuduPredicate(schema, info);
                if (null != kuduPredicate) {
                    builder.addPredicate(kuduPredicate);
                }
                return info;
            }).count();
        }

        if (null != lowerBoundPrimaryKey && null != upperBoundPrimaryKey && null != primaryKeys) {
            List<ColumnSchema> columnSchemas = schema.getPrimaryKeyColumns();
            Map<String, Integer> columnName = new HashMap<String, Integer>(columnSchemas.size());
            for (int i = 0; i < columnSchemas.size(); i++) {
                columnName.put(columnSchemas.get(i).getName(), i);
            }
            String[] primaryKey = splitString(primaryKeys);
            String[] lowerBounds = splitString(lowerBoundPrimaryKey);
            String[] upperBounds = splitString(upperBoundPrimaryKey);
            PartialRow lowerPartialRow = schema.newPartialRow();
            PartialRow upperPartialRow = schema.newPartialRow();
            for (int i = 0; i < primaryKey.length; i++) {
                Integer index = columnName.get(primaryKey[i]);
                KuduUtil.primaryKeyRange(lowerPartialRow, columnSchemas.get(index).getType(), primaryKey[i], lowerBounds[i]);
                KuduUtil.primaryKeyRange(upperPartialRow, columnSchemas.get(index).getType(), primaryKey[i], upperBounds[i]);
            }
            builder.lowerBound(lowerPartialRow);
            builder.exclusiveUpperBound(upperPartialRow);
        }
        List<String> projectColumns = Arrays.asList(sideFieldNames);
        return builder.setProjectedColumnNames(projectColumns).build();
    }

    private String[] splitString(String data) {
        return data.split(",");
    }

    @Override
    public void close() throws Exception {
        //公用一个client  如果每次刷新间隔时间较长可以每次获取一个
        super.close();
        if (null != client) {
            try {
                client.close();
            } catch (Exception e) {
                LOG.error("Error while closing client.", e);
            }
        }
    }
}
