package com.dtstack.flink.sql.side.kudu.table;

import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.BaseSideInfo;
import com.dtstack.flink.sql.side.PredicateInfo;
import com.dtstack.flink.sql.side.kudu.KuduAllSideInfo;
import com.dtstack.flink.sql.side.kudu.utils.KuduUtil;
import com.dtstack.flink.sql.side.table.BaseTableFunction;
import com.dtstack.flink.sql.util.KrbUtils;
import com.dtstack.flink.sql.util.ThreadUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
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

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author tiezhu
 * date 2020/12/3
 * company dtstack
 */
public class KuduTableFunction extends BaseTableFunction {

    private static final Logger LOG = LoggerFactory.getLogger(KuduTableFunction.class);

    /**
     * 缓存条数
     */
    private static final Long FETCH_SIZE = 1000L;

    private KuduClient client;
    private KuduTable table;

    private final KuduSideTableInfo tableInfo;

    public KuduTableFunction(AbstractSideTableInfo sideTableInfo, String[] lookupKeys) {
        super(new KuduAllSideInfo(sideTableInfo, lookupKeys));
        tableInfo = (KuduSideTableInfo) sideInfo.getSideTableInfo();
    }

    @Override
    protected void loadData(Object cacheRef) {
        Map<String, List<Map<String, Object>>> tmpCache = (Map<String, List<Map<String, Object>>>) cacheRef;
        KuduScanner scanner = getKuduScannerWithRetry(tableInfo);
        //load data from table
        if (Objects.isNull(scanner)) {
            throw new NullPointerException("kudu scanner is null");
        }

        while (scanner.hasMoreRows()) {
            try {
                RowResultIterator results = scanner.nextRows();

                if (Objects.isNull(results)) {
                    break;
                }

                while (results.hasNext()) {
                    RowResult result = results.next();
                    Map<String, Object> oneRow = Maps.newHashMap();
                    for (String name : tableInfo.getFields()) {
                        String sideFieldName = name.trim();
                        ColumnSchema columnSchema = table.getSchema().getColumn(sideFieldName);
                        if (Objects.nonNull(columnSchema)) {
                            KuduUtil.setMapValue(columnSchema.getType(), oneRow, sideFieldName, result);
                        }
                    }
                    buildCache(oneRow, tmpCache);
                }
            } catch (KuduException ke) {
                LOG.error("", ke);
            }
        }
        KuduUtil.closeKuduScanner(scanner);
    }

    private KuduScanner getKuduScannerWithRetry(KuduSideTableInfo tableInfo) {
        String connInfo = "kuduMasters:" + tableInfo.getKuduMasters() + ";tableName:" + tableInfo.getTableName();
        for (int i = 0; i < CONN_RETRY_NUM; i++) {
            try {
                if (Objects.isNull(client)) {
                    String tableName = tableInfo.getTableName();
                    client = getClient(tableInfo);
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
                LOG.error("connInfo\n " + connInfo);
                ThreadUtils.sleepMilliseconds(5);
            }
        }
        throw new RuntimeException("Get kudu connect failed! Current Conn Info \n" + connInfo);
    }

    /**
     * get kudu client for scanner
     *
     * @param tableInfo kudu table info
     * @return client
     * @throws IOException ioe
     */
    private KuduClient getClient(KuduSideTableInfo tableInfo) throws IOException {
        String kuduMasters = tableInfo.getKuduMasters();
        Integer workerCount = tableInfo.getWorkerCount();
        Integer defaultOperationTimeoutMs = tableInfo.getDefaultOperationTimeoutMs();

        Preconditions.checkNotNull(kuduMasters, "kuduMasters could not be null");

        KuduClient.KuduClientBuilder kuduClientBuilder = new KuduClient.KuduClientBuilder(kuduMasters);

        if (Objects.nonNull(workerCount)) {
            kuduClientBuilder.workerCount(workerCount);
        }

        if (Objects.nonNull(defaultOperationTimeoutMs)) {
            kuduClientBuilder.defaultOperationTimeoutMs(defaultOperationTimeoutMs);
        }

        if (tableInfo.isEnableKrb()) {
            UserGroupInformation ugi = KrbUtils.loginAndReturnUgi(
                    tableInfo.getPrincipal()
                    , tableInfo.getKeytab()
                    , tableInfo.getKrb5conf());

            return ugi.doAs((PrivilegedAction<KuduClient>) kuduClientBuilder::build);
        } else {
            return kuduClientBuilder.build();
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
        String[] sideFieldNames = tableInfo.getFields();

        if (Objects.isNull(limitNum) || limitNum <= 0) {
            builder.limit(FETCH_SIZE);
        } else {
            builder.limit(limitNum);
        }
        if (Objects.nonNull(batchSizeBytes)) {
            builder.batchSizeBytes(batchSizeBytes);
        }
        if (null != isFaultTolerant) {
            builder.setFaultTolerant(isFaultTolerant);
        }
        //  填充谓词信息
        List<PredicateInfo> predicateInfoList = sideInfo.getSideTableInfo().getPredicateInfoes();
        if (predicateInfoList.size() > 0) {
            predicateInfoList.stream().peek(info -> {
                KuduPredicate kuduPredicate = KuduUtil.buildKuduPredicate(schema, info);
                if (Objects.nonNull(kuduPredicate)) {
                    builder.addPredicate(kuduPredicate);
                }
            }).count();
        }

        List<String> projectColumns = Arrays.asList(sideFieldNames);
        return builder.setProjectedColumnNames(projectColumns).build();
    }

    @Override
    public void close() {
        super.close();
        //公用一个client  如果每次刷新间隔时间较长可以每次获取一个
        if (Objects.nonNull(client)) {
            try {
                client.close();
            } catch (Exception e) {
                LOG.error("Error while closing client.", e);
            }
        }
    }
}
