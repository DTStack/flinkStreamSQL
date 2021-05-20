package com.dtstack.flink.sql.side.kudu;

import com.dtstack.flink.sql.enums.ECacheContentType;
import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.BaseAsyncReqRow;
import com.dtstack.flink.sql.side.CacheMissVal;
import com.dtstack.flink.sql.side.FieldInfo;
import com.dtstack.flink.sql.side.JoinInfo;
import com.dtstack.flink.sql.side.PredicateInfo;
import com.dtstack.flink.sql.side.cache.CacheObj;
import com.dtstack.flink.sql.side.kudu.table.KuduSideTableInfo;
import com.dtstack.flink.sql.side.kudu.utils.KuduUtil;
import com.dtstack.flink.sql.util.DateUtil;
import com.dtstack.flink.sql.util.KrbUtils;
import com.dtstack.flink.sql.util.RowDataComplete;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.AsyncKuduScanner;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.security.PrivilegedAction;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class KuduAsyncReqRow extends BaseAsyncReqRow {

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

    public KuduAsyncReqRow(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, AbstractSideTableInfo sideTableInfo) {
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
    private void connKuDu() throws IOException {
        if (null == table) {
            String tableName = kuduSideTableInfo.getTableName();
            asyncClient = getClient();
            if (!asyncClient.syncClient().tableExists(tableName)) {
                throw new IllegalArgumentException("Table Open Failed , please check table exists");
            }
            table = asyncClient.syncClient().openTable(tableName);
            LOG.info("connect kudu is succeed!");
        }
        scannerBuilder = asyncClient.newScannerBuilder(table);
        Integer batchSizeBytes = kuduSideTableInfo.getBatchSizeBytes();
        Long limitNum = kuduSideTableInfo.getLimitNum();
        Boolean isFaultTolerant = kuduSideTableInfo.getFaultTolerant();
        //查询需要的字段
        String[] sideFieldNames = StringUtils.split(sideInfo.getSideSelectFields(), ",");

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

    private AsyncKuduClient getClient() throws IOException {
        String kuduMasters = kuduSideTableInfo.getKuduMasters();
        Integer workerCount = kuduSideTableInfo.getWorkerCount();
        Integer defaultOperationTimeoutMs = kuduSideTableInfo.getDefaultOperationTimeoutMs();

        Preconditions.checkNotNull(kuduMasters, "kuduMasters could not be null");

        AsyncKuduClient.AsyncKuduClientBuilder asyncKuduClientBuilder = new AsyncKuduClient.AsyncKuduClientBuilder(kuduMasters);
        if (null != workerCount) {
            asyncKuduClientBuilder.workerCount(workerCount);
        }

        if (null != defaultOperationTimeoutMs) {
            asyncKuduClientBuilder.defaultOperationTimeoutMs(defaultOperationTimeoutMs);
        }

        if (kuduSideTableInfo.isEnableKrb()) {
            UserGroupInformation ugi = KrbUtils.loginAndReturnUgi(
                kuduSideTableInfo.getPrincipal(),
                kuduSideTableInfo.getKeytab(),
                kuduSideTableInfo.getKrb5conf()
            );
            return ugi.doAs(
                (PrivilegedAction<AsyncKuduClient>) asyncKuduClientBuilder::build);
        } else {
            return asyncKuduClientBuilder.build();
        }
    }

    @Override
    public void handleAsyncInvoke(Map<String, Object> inputParams, BaseRow input, ResultFuture<BaseRow> resultFuture) throws Exception {
        GenericRow genericRow = (GenericRow) input;
        GenericRow inputCopy = GenericRow.copyReference(genericRow);
        //scannerBuilder 设置为null重新加载过滤条件,然后connkudu重新赋值
        //todo:代码需要优化
        scannerBuilder = null;
        connKuDu();
        Schema schema = table.getSchema();
        //  @wenbaoup fix bug
        inputParams.forEach((key, value) -> {
            Object transformValue = transformValue(value);
            if (transformValue == null) {
                scannerBuilder.addPredicate(
                       KuduPredicate.newIsNullPredicate(schema.getColumn(key))
                );
                return;
            }
            scannerBuilder.addPredicate(
                KuduPredicate.newInListPredicate(
                    schema.getColumn(key),
                    Collections.singletonList(transformValue)
                )
            );
        });

        //  填充谓词信息
        List<PredicateInfo> predicateInfoes = sideInfo.getSideTableInfo().getPredicateInfoes();
        if (predicateInfoes.size() > 0) {
            predicateInfoes.stream().peek(info -> {
                KuduPredicate kuduPredicate = KuduUtil.buildKuduPredicate(schema, info);
                if (null != kuduPredicate) {
                    scannerBuilder.addPredicate(kuduPredicate);
                }
            }).count();
        }

        List<Map<String, Object>> cacheContent = Lists.newArrayList();
        AsyncKuduScanner asyncKuduScanner = scannerBuilder.build();
        List<BaseRow> rowList = Lists.newArrayList();
        Deferred<RowResultIterator> data = asyncKuduScanner.nextRows();
        //从之前的同步修改为调用异步的Callback
        data.addCallbackDeferring(new GetListRowCB(inputCopy, cacheContent, rowList, asyncKuduScanner, resultFuture, buildCacheKey(inputParams)));
    }

    /**
     * 将value转化为Java 通用类型
     * @param value value
     * @return 类型转化的value
     */
    private Object transformValue(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Number && !(value instanceof BigDecimal)) {
            return value;
        } else if (value instanceof Boolean) {
            return value;
        } else if (value instanceof String) {
            return value;
        } else if (value instanceof Character) {
            return value;
        } else if (value instanceof CharSequence) {
            return value;
        } else if (value instanceof Map) {
            return value;
        } else if (value instanceof List) {
            return value;
        } else if (value instanceof byte[]) {
            return value;
        } else if (value instanceof Instant) {
            return value;
        } else if (value instanceof Timestamp) {
            value = DateUtil.timestampToString((Timestamp) value);
        } else if (value instanceof java.util.Date) {
            value = DateUtil.dateToString((java.sql.Date) value);
        } else {
            value = value.toString();
        }
        return value;
    }

    @Override
    public String buildCacheKey(Map<String, Object> inputParams) {
        StringBuilder sb = new StringBuilder();
        for (Object ele : inputParams.values()) {
            sb.append(ele)
                    .append("_");
        }

        return sb.toString();
    }


    @Override
    public BaseRow fillData(BaseRow input, Object sideInput) {
        GenericRow genericRow = (GenericRow) input;
        Map<String, Object> cacheInfo = (Map<String, Object>) sideInput;
        GenericRow row = new GenericRow(sideInfo.getOutFieldInfoList().size());
        row.setHeader(genericRow.getHeader());
        for (Map.Entry<Integer, Integer> entry : sideInfo.getInFieldIndex().entrySet()) {
            Object obj = genericRow.getField(entry.getValue());
            obj = convertTimeIndictorTypeInfo(entry.getValue(), obj);
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

    class GetListRowCB implements Callback<Deferred<List<Row>>, RowResultIterator> {
        private BaseRow input;
        private List<Map<String, Object>> cacheContent;
        private List<BaseRow> rowList;
        private AsyncKuduScanner asyncKuduScanner;
        private ResultFuture<BaseRow> resultFuture;
        private String key;


        public GetListRowCB() {
        }

        GetListRowCB(BaseRow input, List<Map<String, Object>> cacheContent, List<BaseRow> rowList,
                     AsyncKuduScanner asyncKuduScanner, ResultFuture<BaseRow> resultFuture, String key) {
            this.input = input;
            this.cacheContent = cacheContent;
            this.rowList = rowList;
            this.asyncKuduScanner = asyncKuduScanner;
            this.resultFuture = resultFuture;
            this.key = key;
        }

        @Override
        public Deferred<List<Row>> call(RowResultIterator results) throws Exception {
            if (results == null) {
                dealMissKey(input, resultFuture);
                return null;
            }

            for (RowResult result : results) {
                Map<String, Object> oneRow = Maps.newHashMap();
                for (String sideFieldName1 : StringUtils.split(sideInfo.getSideSelectFields(), ",")) {
                    String sideFieldName = sideFieldName1.trim();
                    ColumnSchema columnSchema = table.getSchema().getColumn(sideFieldName);
                    if (null != columnSchema) {
                        oneRow.put(sideFieldName, result.getObject(sideFieldName));
                    }
                }
                BaseRow row = fillData(input, oneRow);
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
                RowDataComplete.completeBaseRow(resultFuture, rowList);
            } else {
                dealMissKey(input, resultFuture);
                if (openCache()) {
                    //放置在putCache的Miss中 一段时间内同一个key都会直接返回
                    putCache(key, CacheMissVal.getMissKeyObj());
                }
            }

            resultFuture.complete(Collections.emptyList());
            return null;
        }
    }

}
