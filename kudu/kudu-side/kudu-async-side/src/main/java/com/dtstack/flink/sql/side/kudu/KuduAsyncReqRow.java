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
import com.dtstack.flink.sql.util.KrbUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
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
import java.security.PrivilegedAction;
import java.sql.Timestamp;
import java.util.*;

public class KuduAsyncReqRow extends BaseAsyncReqRow {

    private static final Logger LOG = LoggerFactory.getLogger(KuduAsyncReqRow.class);

    private static final TimeZone LOCAL_TZ = TimeZone.getDefault();

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
            LOG.info("connect kudu is successed!");
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
                    new PrivilegedAction<AsyncKuduClient>() {
                        @Override
                        public AsyncKuduClient run() {
                            return asyncKuduClientBuilder.build();
                        }
                    });
        } else {
            return asyncKuduClientBuilder.build();
        }
    }

    @Override
    public void handleAsyncInvoke(Map<String, Object> inputParams, CRow input, ResultFuture<CRow> resultFuture) throws Exception {
        CRow inputCopy = new CRow(input.row(), input.change());
        //scannerBuilder 设置为null重新加载过滤条件,然后connkudu重新赋值
        //todo:代码需要优化
        scannerBuilder = null;
        connKuDu();
        Schema schema = table.getSchema();
        //  @wenbaoup fix bug
        inputParams.entrySet().forEach(e ->{
            scannerBuilder.addPredicate(KuduPredicate.newInListPredicate(schema.getColumn(e.getKey()), Collections.singletonList(e.getValue())));
        });

        //  填充谓词信息
        List<PredicateInfo> predicateInfoes = sideInfo.getSideTableInfo().getPredicateInfoes();
        if (predicateInfoes.size() > 0) {
            predicateInfoes.stream().map(info -> {
                KuduPredicate kuduPredicate = KuduUtil.buildKuduPredicate(schema, info);
                if (null != kuduPredicate) {
                    scannerBuilder.addPredicate(kuduPredicate);
                }
                return info;
            }).count();
        }

        List<Map<String, Object>> cacheContent = Lists.newArrayList();
        AsyncKuduScanner asyncKuduScanner = scannerBuilder.build();
        List<CRow> rowList = Lists.newArrayList();
        Deferred<RowResultIterator> data = asyncKuduScanner.nextRows();
        //从之前的同步修改为调用异步的Callback
        data.addCallbackDeferring(new GetListRowCB(inputCopy, cacheContent, rowList, asyncKuduScanner, resultFuture, buildCacheKey(inputParams)));
    }

    @Override
    public String buildCacheKey(Map<String, Object> inputParams) {
        StringBuilder sb = new StringBuilder();
        for (Object ele : inputParams.values()) {
            sb.append(ele.toString())
                    .append("_");
        }

        return sb.toString();
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
                //去除上一层OutputRowtimeProcessFunction 调用时区导致的影响
                obj = ((Timestamp) obj).getTime() + (long)LOCAL_TZ.getOffset(((Timestamp) obj).getTime());
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
        private CRow input;
        private List<Map<String, Object>> cacheContent;
        private List<CRow> rowList;
        private AsyncKuduScanner asyncKuduScanner;
        private ResultFuture<CRow> resultFuture;
        private String key;


        public GetListRowCB() {
        }

        GetListRowCB(CRow input, List<Map<String, Object>> cacheContent, List<CRow> rowList, AsyncKuduScanner asyncKuduScanner, ResultFuture<CRow> resultFuture, String key) {
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
                for (String sideFieldName1 : StringUtils.split(sideInfo.getSideSelectFields(), ",")) {
                    String sideFieldName = sideFieldName1.trim();
                    ColumnSchema columnSchema = table.getSchema().getColumn(sideFieldName);
                    if (null != columnSchema) {
                        KuduUtil.setMapValue(columnSchema.getType(), oneRow, sideFieldName, result);
                    }
                }
                Row row = fillData(input.row(), oneRow);
                if (openCache()) {
                    cacheContent.add(oneRow);
                }
                rowList.add(new CRow(row, input.change()));
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
