package com.dtstack.flink.sql.side.redis.table;

import com.dtstack.flink.sql.enums.ECacheContentType;
import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.BaseSideInfo;
import com.dtstack.flink.sql.side.CacheMissVal;
import com.dtstack.flink.sql.side.cache.CacheObj;
import com.dtstack.flink.sql.side.redis.RedisAsyncSideInfo;
import com.dtstack.flink.sql.side.redis.enums.RedisType;
import com.dtstack.flink.sql.side.table.BaseAsyncTableFunction;
import com.dtstack.flink.sql.util.SwitchUtil;
import com.google.common.collect.Lists;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisHashAsyncCommands;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author tiezhu
 * date 2020/12/2
 * company dtstack
 */
public class RedisAsyncTableFunction extends BaseAsyncTableFunction {

    private static final Logger LOG = LoggerFactory.getLogger(RedisAsyncTableFunction.class);

    private RedisClient redisClient;
    private StatefulRedisConnection<String, String> connection;
    private RedisClusterClient clusterClient;
    private StatefulRedisClusterConnection<String, String> clusterConnection;
    private RedisKeyAsyncCommands<String, String> async;
    private RedisSideTableInfo redisSideTableInfo;

    private String keyPrefix;

    public RedisAsyncTableFunction(BaseSideInfo sideInfo) {
        super(sideInfo);
    }

    public RedisAsyncTableFunction(AbstractSideTableInfo sideTableInfo, String[] lookupKeys) {
        super(new RedisAsyncSideInfo(sideTableInfo, lookupKeys));
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        redisSideTableInfo = (RedisSideTableInfo) sideInfo.getSideTableInfo();
        buildRedisClient(redisSideTableInfo);
        keyPrefix = buildKeyPrefix(redisSideTableInfo.getTableName(), redisSideTableInfo.getPrimaryKeys());
    }

    @Override
    public void handleAsyncInvoke(CompletableFuture<Collection<Row>> future, Object... keys) throws Exception {
        String key = buildCacheKey(keys);
        if (StringUtils.isBlank(key)) {
            return;
        }
        RedisFuture<Map<String, String>> redisFuture = ((RedisHashAsyncCommands) async).hgetall(key);
        redisFuture.thenAccept(resultValues -> {
            List<Object> cacheContent = Lists.newArrayList();
            List<Row> rowList = Lists.newArrayList();
            List<Object> results = Lists.newArrayList();
            if (MapUtils.isNotEmpty(resultValues)) {
                results.add(resultValues);
                rowList.addAll(getRows(cacheContent, results));
                dealCacheData(key, CacheObj.buildCacheObj(ECacheContentType.MultiLine, cacheContent));
                future.complete(rowList);
            } else {
                dealMissKey(future);
                dealCacheData(key, CacheMissVal.getMissKeyObj());
            }
        });
    }

    protected List<Row> getRows(List<Object> cacheContent, List<Object> results) {
        List<Row> rowList = Lists.newArrayList();
        for (Object line : results) {
            Row row = fillData(line);
            if (null != cacheContent && openCache()) {
                cacheContent.add(line);
            }
            rowList.add(row);
        }
        return rowList;
    }

    @Override
    public String buildCacheKey(Object... keys) {
        AtomicReference<String> replace = new AtomicReference<>();
        Arrays.stream(keys)
                .map(String::valueOf)
                .forEach(key -> replace.set(StringUtils.replace(keyPrefix, "*", key)));
        return replace.get();
    }

    @Override
    protected void fillDataWapper(Object sideInput, String[] sideFieldNames, String[] sideFieldTypes, Row row) {
        Map<String, Object> values = (Map<String, Object>) sideInput;
        for (int i = 0; i < sideFieldNames.length; i++) {
            row.setField(i, SwitchUtil.getTarget(values.get(sideFieldNames[i].trim()), sideFieldTypes[i]));
        }
    }

    /**
     * 构建redis key tableName_key1_key2
     *
     * @param tableName tableName
     * @param keys      key1, key2...
     * @return key like tableName_key1_key2
     */
    private String buildKeyPrefix(String tableName, Object... keys) {
        StringBuilder keyPattern = new StringBuilder(tableName);
        for (Object ignored : keys) {
            keyPattern.append("_").append("*");
        }
        return keyPattern.toString();
    }

    private void buildRedisClient(RedisSideTableInfo tableInfo) {
        String url = redisSideTableInfo.getUrl();
        String password = Objects.isNull(redisSideTableInfo.getPassword()) ?
                "" : redisSideTableInfo.getPassword();
        String database = redisSideTableInfo.getDatabase();

        if (Objects.isNull(database)) {
            database = "0";
        }
        switch (RedisType.parse(tableInfo.getRedisType())) {
            case STANDALONE:
                RedisURI redisUri = RedisURI.create("redis://" + url);
                redisUri.setPassword(password);
                redisUri.setDatabase(Integer.parseInt(database));
                redisClient = RedisClient.create(redisUri);
                connection = redisClient.connect();
                async = connection.async();
                break;
            case SENTINEL:
                RedisURI redisSentinelUri = RedisURI.create("redis-sentinel://" + url);
                redisSentinelUri.setPassword(password);
                redisSentinelUri.setDatabase(Integer.parseInt(database));
                redisSentinelUri.setSentinelMasterId(redisSideTableInfo.getMasterName());
                redisClient = RedisClient.create(redisSentinelUri);
                connection = redisClient.connect();
                async = connection.async();
                break;
            case CLUSTER:
                RedisURI clusterUri = RedisURI.create("redis://" + url);
                clusterUri.setPassword(password);
                clusterClient = RedisClusterClient.create(clusterUri);
                clusterConnection = clusterClient.connect();
                async = clusterConnection.async();
            default:
                LOG.error("Illegal Argument of Redis Type: " + tableInfo.getRedisType());
                break;
        }
    }

    @Override
    public void close() throws Exception {
        if (Objects.nonNull(redisClient)) {
            redisClient.shutdown();
        }

        if (Objects.nonNull(connection)) {
            connection.close();
        }

        if (Objects.nonNull(clusterClient)) {
            clusterClient.shutdown();
        }

        if (Objects.nonNull(clusterConnection)) {
            clusterConnection.close();
        }
    }
}
