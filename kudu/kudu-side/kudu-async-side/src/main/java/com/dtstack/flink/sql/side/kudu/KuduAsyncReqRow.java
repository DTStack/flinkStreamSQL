package com.dtstack.flink.sql.side.kudu;

import com.dtstack.flink.sql.enums.ECacheContentType;
import com.dtstack.flink.sql.side.*;
import com.dtstack.flink.sql.side.cache.CacheObj;
import com.dtstack.flink.sql.side.kudu.table.KuduSideTableInfo;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import io.vertx.core.json.JsonArray;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import com.google.common.collect.Lists;
import org.apache.flink.configuration.Configuration;
import com.google.common.collect.Maps;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class KuduAsyncReqRow extends AsyncReqRow {

    private static final Logger LOG = LoggerFactory.getLogger(KuduAsyncReqRow.class);
    /**
     * 获取连接的尝试次数
     */
    private static final int CONN_RETRY_NUM = 3;
    /**
     * 缓存条数
     */
    private static final Long FETCH_SIZE = 1000L;

    private static final long serialVersionUID = 5028583854989267753L;


    private AsyncKuduClient asyncClient;

    private KuduTable table;

    private KuduSideTableInfo kuduSideTableInfo;

    private AsyncKuduScanner.AsyncKuduScannerBuilder scannerBuilder;

    public KuduAsyncReqRow(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) {
        super(new KuduAsyncSideInfo(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo));
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        kuduSideTableInfo = (KuduSideTableInfo) sideInfo.getSideTableInfo();
        connKuDu();
    }

    /**
     * 连接kudu中的表
     *
     * @throws KuduException
     */
    private void connKuDu() throws KuduException {
        if (null == table) {
            String kuduMasters = kuduSideTableInfo.getKuduMasters();
            String tableName = kuduSideTableInfo.getTableName();
            Integer workerCount = kuduSideTableInfo.getWorkerCount();
            Integer defaultSocketReadTimeoutMs = kuduSideTableInfo.getDefaultSocketReadTimeoutMs();
            Integer defaultOperationTimeoutMs = kuduSideTableInfo.getDefaultOperationTimeoutMs();

            Preconditions.checkNotNull(kuduMasters, "kuduMasters could not be null");

            AsyncKuduClient.AsyncKuduClientBuilder asyncKuduClientBuilder = new AsyncKuduClient.AsyncKuduClientBuilder(kuduMasters);
            if (null != workerCount) {
                asyncKuduClientBuilder.workerCount(workerCount);
            }
            if (null != defaultSocketReadTimeoutMs) {
                asyncKuduClientBuilder.defaultSocketReadTimeoutMs(defaultSocketReadTimeoutMs);
            }

            if (null != defaultOperationTimeoutMs) {
                asyncKuduClientBuilder.defaultOperationTimeoutMs(defaultOperationTimeoutMs);
            }
            asyncClient = asyncKuduClientBuilder.build();
            if (!asyncClient.syncClient().tableExists(tableName)) {
                throw new IllegalArgumentException("Table Open Failed , please check table exists");
            }
            table = asyncClient.syncClient().openTable(tableName);
            LOG.info("connect kudu is successed!");
        }
        scannerBuilder = asyncClient.newScannerBuilder(table);
        Integer batchSizeBytes = kuduSideTableInfo.getBatchSizeBytes();
        Long limitNum = kuduSideTableInfo.getLimitNum();
        Boolean isFaultTolerant = kuduSideTableInfo.getFaultTolerant();
        //查询需要的字段
        String[] sideFieldNames = sideInfo.getSideSelectFields().split(",");

        if (null == limitNum || limitNum <= 0) {
            scannerBuilder.limit(FETCH_SIZE);
        } else {
            scannerBuilder.limit(limitNum);
        }
        if (null != batchSizeBytes) {
            scannerBuilder.batchSizeBytes(batchSizeBytes);
        }
        if (null != isFaultTolerant) {
            scannerBuilder.setFaultTolerant(isFaultTolerant);
        }

        List<String> projectColumns = Arrays.asList(sideFieldNames);
        scannerBuilder.setProjectedColumnNames(projectColumns);
    }


    @Override
    public void asyncInvoke(Row input, ResultFuture<Row> resultFuture) throws Exception {
        //scannerBuilder 设置为null重新加载过滤条件
        scannerBuilder = null;
        connKuDu();
        JsonArray inputParams = new JsonArray();
        Schema schema = table.getSchema();

        for (int i = 0; i < sideInfo.getEqualValIndex().size(); i++) {
            Object equalObj = input.getField(sideInfo.getEqualValIndex().get(i));
            if (equalObj == null) {
                resultFuture.complete(null);
                return;
            }
            //增加过滤条件
            scannerBuilder.addPredicate(KuduPredicate.newInListPredicate(schema.getColumn(sideInfo.getEqualFieldList().get(i)), Collections.singletonList(equalObj)));
            inputParams.add(equalObj);
        }

        String key = buildCacheKey(inputParams);

        if (openCache()) {
            //判断数据是否已经加载到缓存中
            CacheObj val = getFromCache(key);
            if (val != null) {

                if (ECacheContentType.MissVal == val.getType()) {
                    dealMissKey(input, resultFuture);
                    return;
                } else if (ECacheContentType.SingleLine == val.getType()) {
                    Row row = fillData(input, val);
                    resultFuture.complete(Collections.singleton(row));
                } else if (ECacheContentType.MultiLine == val.getType()) {
                    List<Row> rowList = Lists.newArrayList();
                    for (Object jsonArray : (List) val.getContent()) {
                        Row row = fillData(input, jsonArray);
                        rowList.add(row);
                    }
                    resultFuture.complete(rowList);
                } else {
                    throw new RuntimeException("not support cache obj type " + val.getType());
                }
                return;
            }
        }
        List<Map<String, Object>> cacheContent = Lists.newArrayList();
        AsyncKuduScanner asyncKuduScanner = scannerBuilder.build();
        List<Row> rowList = Lists.newArrayList();
        Deferred<RowResultIterator> data = asyncKuduScanner.nextRows();
        //从之前的同步修改为调用异步的Callback
        data.addCallbackDeferring(new GetListRowCB(input, cacheContent, rowList, asyncKuduScanner, resultFuture, key));
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

    public String buildCacheKey(JsonArray jsonArray) {
        StringBuilder sb = new StringBuilder();
        for (Object ele : jsonArray.getList()) {
            sb.append(ele.toString())
                    .append("_");
        }

        return sb.toString();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (null != asyncClient) {
            try {
                asyncClient.close();
            } catch (Exception e) {
                LOG.error("Error while closing client.", e);
            }
        }
    }

    private void setMapValue(Type type, Map<String, Object> oneRow, String sideFieldName, RowResult result) {
        switch (type) {
            case STRING:
                oneRow.put(sideFieldName, result.getString(sideFieldName));
                break;
            case FLOAT:
                oneRow.put(sideFieldName, result.getFloat(sideFieldName));
                break;
            case INT8:
                oneRow.put(sideFieldName, (int) result.getByte(sideFieldName));
                break;
            case INT16:
                oneRow.put(sideFieldName, (int) result.getShort(sideFieldName));
                break;
            case INT32:
                oneRow.put(sideFieldName, result.getInt(sideFieldName));
                break;
            case INT64:
                oneRow.put(sideFieldName, result.getLong(sideFieldName));
                break;
            case DOUBLE:
                oneRow.put(sideFieldName, result.getDouble(sideFieldName));
                break;
            case BOOL:
                oneRow.put(sideFieldName, result.getBoolean(sideFieldName));
                break;
            case UNIXTIME_MICROS:
                oneRow.put(sideFieldName, result.getTimestamp(sideFieldName));
                break;
            case BINARY:
                oneRow.put(sideFieldName, result.getBinary(sideFieldName));
                break;
            default:
                throw new IllegalArgumentException("Illegal var type: " + type);
        }
    }

    class GetListRowCB implements Callback<Deferred<List<Row>>, RowResultIterator> {
        private Row input;
        private List<Map<String, Object>> cacheContent;
        private List<Row> rowList;
        private AsyncKuduScanner asyncKuduScanner;
        private ResultFuture<Row> resultFuture;
        private String key;


        public GetListRowCB() {
        }

        GetListRowCB(Row input, List<Map<String, Object>> cacheContent, List<Row> rowList, AsyncKuduScanner asyncKuduScanner, ResultFuture<Row> resultFuture, String key) {
            this.input = input;
            this.cacheContent = cacheContent;
            this.rowList = rowList;
            this.asyncKuduScanner = asyncKuduScanner;
            this.resultFuture = resultFuture;
            this.key = key;
        }

        @Override
        public Deferred<List<Row>> call(RowResultIterator results) throws Exception {
            for (RowResult result : results) {
                Map<String, Object> oneRow = Maps.newHashMap();
                for (String sideFieldName1 : sideInfo.getSideSelectFields().split(",")) {
                    String sideFieldName = sideFieldName1.trim();
                    ColumnSchema columnSchema = table.getSchema().getColumn(sideFieldName);
                    if (null != columnSchema) {
                        setMapValue(columnSchema.getType(), oneRow, sideFieldName, result);
                    }
                }
                Row row = fillData(input, oneRow);
                if (openCache()) {
                    cacheContent.add(oneRow);
                }
                rowList.add(row);
            }
            if (asyncKuduScanner.hasMoreRows()) {
                return asyncKuduScanner.nextRows().addCallbackDeferring(this);
            }

            if (rowList.size() > 0) {
                if (openCache()) {
                    putCache(key, CacheObj.buildCacheObj(ECacheContentType.MultiLine, cacheContent));
                }
                resultFuture.complete(rowList);
            } else {
                dealMissKey(input, resultFuture);
                if (openCache()) {
                    //放置在putCache的Miss中 一段时间内同一个key都会直接返回
                    putCache(key, CacheMissVal.getMissKeyObj());
                }
            }

            return null;
        }
    }

}
