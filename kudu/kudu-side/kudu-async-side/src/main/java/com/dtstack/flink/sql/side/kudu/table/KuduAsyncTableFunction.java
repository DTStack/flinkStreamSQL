/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flink.sql.side.kudu.table;

import com.dtstack.flink.sql.enums.ECacheContentType;
import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.CacheMissVal;
import com.dtstack.flink.sql.side.PredicateInfo;
import com.dtstack.flink.sql.side.cache.CacheObj;
import com.dtstack.flink.sql.side.kudu.KuduAsyncSideInfo;
import com.dtstack.flink.sql.side.kudu.utils.KuduUtil;
import com.dtstack.flink.sql.side.table.BaseAsyncTableFunction;
import com.dtstack.flink.sql.util.KrbUtils;
import com.dtstack.flink.sql.util.SwitchUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.AsyncKuduScanner;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * @author tiezhu
 * date 2020/12/9
 * company dtstack
 */
public class KuduAsyncTableFunction extends BaseAsyncTableFunction {

    private static final Logger LOG = LoggerFactory.getLogger(KuduAsyncTableFunction.class);
    private final KuduSideTableInfo kuduSideTableInfo;

    private AsyncKuduClient asyncClient;
    private KuduTable table;
    private AsyncKuduScanner.AsyncKuduScannerBuilder scannerBuilder;

    /**
     * 缓存条数
     */
    private static final Long FETCH_SIZE = 1000L;

    public KuduAsyncTableFunction(AbstractSideTableInfo sideTableInfo, String[] lookupKeys) {
        super(new KuduAsyncSideInfo(sideTableInfo, lookupKeys));
        kuduSideTableInfo = (KuduSideTableInfo) sideInfo.getSideTableInfo();
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        getKudu();
    }

    private void checkKuduTable() throws IOException {
        try {
            if (Objects.isNull(table)) {
                String tableName = kuduSideTableInfo.getTableName();
                asyncClient = getClient();
                if (!asyncClient.syncClient().tableExists(tableName)) {
                    throw new IllegalArgumentException("Table Open Failed , please check table exists");
                }
                table = asyncClient.syncClient().openTable(tableName);
                LOG.info("connect kudu is succeed!");
            }
        } catch (Exception e) {
            throw new IOException("kudu table error!", e);
        }
    }

    private void getKudu() throws IOException {
        checkKuduTable();
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
        if (Objects.nonNull(batchSizeBytes)) {
            scannerBuilder.batchSizeBytes(batchSizeBytes);
        }
        if (Objects.nonNull(isFaultTolerant)) {
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
        if (Objects.nonNull(workerCount)) {
            asyncKuduClientBuilder.workerCount(workerCount);
        }

        if (Objects.nonNull(defaultOperationTimeoutMs)) {
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

    private AsyncKuduScanner.AsyncKuduScannerBuilder buildAsyncKuduScannerBuilder(Schema schema, Object... keys) throws IOException {
        checkKuduTable();
        AsyncKuduScanner.AsyncKuduScannerBuilder scannerBuilder = asyncClient.newScannerBuilder(table);;
        for (int i = 0; i < keys.length; i++) {
            scannerBuilder.addPredicate(KuduPredicate.newInListPredicate(
                    schema.getColumn(kuduSideTableInfo.getPrimaryKeys().get(i))
                    , Collections.singletonList(keys[i])
            ));
        }
        Integer batchSizeBytes = kuduSideTableInfo.getBatchSizeBytes();
        Long limitNum = kuduSideTableInfo.getLimitNum();
        Boolean isFaultTolerant = kuduSideTableInfo.getFaultTolerant();

        if (Objects.isNull(limitNum) || limitNum <= 0) {
            scannerBuilder.limit(FETCH_SIZE);
        } else {
            scannerBuilder.limit(limitNum);
        }
        if (Objects.nonNull(batchSizeBytes)) {
            scannerBuilder.batchSizeBytes(batchSizeBytes);
        }
        if (Objects.nonNull(isFaultTolerant)) {
            scannerBuilder.setFaultTolerant(isFaultTolerant);
        }

        List<String> projectColumns = Arrays.asList(sideTableInfo.getFields());
        scannerBuilder.setProjectedColumnNames(projectColumns);
        return scannerBuilder;
    }

    @Override
    public void handleAsyncInvoke(CompletableFuture<Collection<Row>> future, Object... keys) throws Exception {
        String key = buildCacheKey(keys);

        if (StringUtils.isBlank(key)) {
            return;
        }
        Schema schema = table.getSchema();
        //scannerBuilder 设置为null重新加载过滤条件,然后connKudu重新赋值
        scannerBuilder = buildAsyncKuduScannerBuilder(schema, keys);
        //  填充谓词信息
        List<PredicateInfo> predicateInfoList = sideTableInfo.getPredicateInfoes();
        if (predicateInfoList.size() > 0) {
            predicateInfoList.stream().peek(info -> {
                KuduPredicate kuduPredicate = KuduUtil.buildKuduPredicate(schema, info);
                if (null != kuduPredicate) {
                    scannerBuilder.addPredicate(kuduPredicate);
                }
            }).count();
        }

        List<Map<String, Object>> cacheContent = Lists.newArrayList();
        AsyncKuduScanner asyncKuduScanner = scannerBuilder.build();
        List<Row> rowList = Lists.newArrayList();

        Deferred<RowResultIterator> iteratorDeferred = asyncKuduScanner.nextRows();
        iteratorDeferred.addCallback(
                new GetListRow(
                        cacheContent
                        , rowList
                        , asyncKuduScanner
                        , future
                        , buildCacheKey(keys)
                )
        );

    }

    @Override
    protected void fillDataWapper(Object sideInput, String[] sideFieldNames, String[] sideFieldTypes, Row row) {
        Map<String, Object> cacheInfo = (Map<String, Object>) sideInput;

        for (int i = 0; i < sideFieldNames.length; i++) {
            if (Objects.isNull(cacheInfo)) {
                row.setField(i, null);
            } else {
                row.setField(i, SwitchUtil.getTarget(
                        cacheInfo.get(sideFieldNames[i].trim()), sideFieldTypes[i]
                ));
            }
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (Objects.nonNull(asyncClient)) {
            try {
                asyncClient.close();
            } catch (Exception e) {
                LOG.error("Error while closing client.", e);
            }
        }

        if (Objects.nonNull(table)) {
            table = null;
        }

        if (Objects.nonNull(scannerBuilder)) {
            scannerBuilder = null;
        }
    }

    private class GetListRow implements Callback<Deferred<List<Row>>, RowResultIterator> {
        private final List<Map<String, Object>> cacheContent;
        private final List<Row> rowList;
        private final AsyncKuduScanner asyncKuduScanner;
        private final CompletableFuture<Collection<Row>> future;
        private final String key;

        public GetListRow(
                List<Map<String, Object>> cacheContent
                , List<Row> rowList
                , AsyncKuduScanner asyncKuduScanner
                , CompletableFuture<Collection<Row>> future
                , String key) {
            this.cacheContent = cacheContent;
            this.rowList = rowList;
            this.asyncKuduScanner = asyncKuduScanner;
            this.future = future;
            this.key = key;
        }

        @Override
        public Deferred<List<Row>> call(RowResultIterator results) {
            for (RowResult result : results) {
                Map<String, Object> oneRow = Maps.newHashMap();
                for (String sideFieldName : kuduSideTableInfo.getFields()) {
                    ColumnSchema columnSchema = table.getSchema().getColumn(sideFieldName);
                    if (Objects.nonNull(columnSchema)) {
                        KuduUtil.setMapValue(columnSchema.getType(), oneRow, sideFieldName, result);
                    }
                }
                Row row = fillData(oneRow);
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
                future.complete(rowList);
            } else {
                dealMissKey(future);
                if (openCache()) {
                    //放置在putCache的Miss中 一段时间内同一个key都会直接返回
                    putCache(key, CacheMissVal.getMissKeyObj());
                }
            }
            return null;
        }
    }
}
