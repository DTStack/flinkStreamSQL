package com.dtstack.flink.sql.side.redis.table;

import com.dtstack.flink.sql.side.AbstractSideTableInfo;
import com.dtstack.flink.sql.side.redis.RedisAllSideInfo;
import com.dtstack.flink.sql.side.redis.enums.RedisType;
import com.dtstack.flink.sql.side.table.BaseTableFunction;
import com.dtstack.flink.sql.util.SwitchUtil;
import com.esotericsoftware.minlog.Log;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisSentinelPool;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author tiezhu
 * date 2020/12/2
 * company dtstack
 */
public class RedisTableFunction extends BaseTableFunction {

    private static final Logger LOG = LoggerFactory.getLogger(RedisTableFunction.class);

    private JedisPool pool;
    private JedisSentinelPool jedisSentinelPool;

    private final RedisSideTableInfo tableInfo;

    private String keyPrefix;

    public RedisTableFunction(AbstractSideTableInfo sideTableInfo, String[] lookupKeys) {
        super(new RedisAllSideInfo(sideTableInfo, lookupKeys));
        tableInfo = (RedisSideTableInfo) sideInfo.getSideTableInfo();
    }

    @Override
    protected void initCache() throws SQLException {
        keyPrefix = buildKeyPrefix(tableInfo.getTableName(), tableInfo.getPrimaryKeys());
        Map<String, List<Map<String, Object>>> newCache = Maps.newConcurrentMap();
        cacheRef.set(newCache);
        loadData(newCache);
    }

    @Override
    protected void reloadCache() {
        Map<String, List<Map<String, Object>>> newCache = Maps.newConcurrentMap();
        try {
            loadData(newCache);
        } catch (Exception e) {
            LOG.error("", e);
        }

        cacheRef.set(newCache);
        LOG.info("----- Redis all cacheRef reload end: {}", Calendar.getInstance());
    }

    @Override
    protected void loadData(Object cacheRef) {
        Map<String, List<Map<String, Object>>> tmpCache = (Map<String, List<Map<String, Object>>>) cacheRef;
        Map<String, String> fieldMapType = Maps.newHashMap();
        String[] fields = tableInfo.getFields();
        String[] fieldTypes = tableInfo.getFieldTypes();
        for (int i = 0; i < fields.length; i++) {
            fieldMapType.put(fields[i], fieldTypes[i]);
        }
        JedisCommands jedis = null;
        try {

            jedis = getJedisWithRetry(CONN_RETRY_NUM);
            if (null == jedis) {
                throw new RuntimeException("redis all load data error,get jedis commands error!");
            }
            Set<String> keys = getRedisKeys(RedisType.parse(tableInfo.getRedisType()), jedis, keyPrefix);
            if (CollectionUtils.isEmpty(keys)) {
                return;
            }
            for (String key : keys) {
                Map<String, Object> resultRow = Maps.newHashMap();
                Map<String, String> hgetAll = jedis.hgetAll(key);
                // 防止一条数据有问题，后面数据无法加载
                try {
                    for (String hkey : hgetAll.keySet()) {
                        resultRow.put(hkey, SwitchUtil.getTarget(hgetAll.get(hkey), fieldMapType.get(hkey)));
                    }
                    tmpCache.computeIfAbsent(key, tmpKey -> Lists.newArrayList()).add(resultRow);
                } catch (Exception e) {
                    LOG.error("", e);
                }
            }
        } catch (Exception e) {
            LOG.error("", e);
        } finally {
            if (jedis != null) {
                try {
                    ((Closeable) jedis).close();
                } catch (IOException e) {
                    Log.error("", e);
                }
            }
            if (jedisSentinelPool != null) {
                jedisSentinelPool.close();
            }
            if (pool != null) {
                pool.close();
            }
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

    /**
     * 每条数据都会进入该方法
     *
     * @param keys 维表join key的值
     */
    @Override
    public void eval(Object... keys) {
        AtomicReference<String> replace = new AtomicReference<>();
        Arrays.stream(keys)
                .map(String::valueOf)
                .forEach(key -> replace.set(StringUtils.replace(keyPrefix, "*", key)));
        List<Map<String, Object>> cacheList = cacheRef.get().get(replace.get());
        // 有数据才往下发，(左/内)连接flink会做相应的处理
        if (!CollectionUtils.isEmpty(cacheList)) {
            cacheList.forEach(one -> collect(fillData(one)));
        }
    }

    private JedisCommands getJedis(RedisSideTableInfo tableInfo) {
        String url = tableInfo.getUrl();
        String password = tableInfo.getPassword();
        String database = Objects.isNull(tableInfo.getDatabase()) ? "0" : tableInfo.getDatabase();
        int timeout = tableInfo.getTimeout();
        if (timeout == 0) {
            timeout = 1000;
        }

        String[] nodes = StringUtils.split(url, ",");
        String[] firstIpPort = StringUtils.split(nodes[0], ":");
        String firstIp = firstIpPort[0];
        String firstPort = firstIpPort[1];
        Set<HostAndPort> addresses = new HashSet<>();
        Set<String> ipPorts = new HashSet<>();
        for (String ipPort : nodes) {
            ipPorts.add(ipPort);
            String[] ipPortPair = ipPort.split(":");
            addresses.add(new HostAndPort(ipPortPair[0].trim(), Integer.parseInt(ipPortPair[1].trim())));
        }

        JedisCommands jedis = null;
        GenericObjectPoolConfig poolConfig = setPoolConfig(tableInfo.getMaxTotal(), tableInfo.getMaxIdle(), tableInfo.getMinIdle());
        switch (RedisType.parse(tableInfo.getRedisType())) {
            //单机
            case STANDALONE:
                pool = new JedisPool(poolConfig, firstIp, Integer.parseInt(firstPort), timeout, password, Integer.parseInt(database));
                jedis = pool.getResource();
                break;
            //哨兵
            case SENTINEL:
                jedisSentinelPool = new JedisSentinelPool(tableInfo.getMasterName(), ipPorts, poolConfig, timeout, password, Integer.parseInt(database));
                jedis = jedisSentinelPool.getResource();
                break;
            //集群
            case CLUSTER:
                jedis = new JedisCluster(addresses, timeout, timeout, 1, poolConfig);
            default:
                break;
        }

        return jedis;
    }

    private JedisCommands getJedisWithRetry(int retryNum) {
        while (retryNum-- > 0) {
            try {
                return getJedis(tableInfo);
            } catch (Exception e) {
                if (retryNum <= 0) {
                    throw new RuntimeException("getJedisWithRetry error", e);
                }
                try {
                    String jedisInfo = "url:" + tableInfo.getUrl() + ",database:" + tableInfo.getDatabase();
                    LOG.warn("get conn fail, wait for 5 sec and try again, connInfo:" + jedisInfo);
                    Thread.sleep(LOAD_DATA_ERROR_SLEEP_TIME);
                } catch (InterruptedException e1) {
                    LOG.error("", e1);
                }
            }
        }
        return null;
    }

    private Set<String> getRedisKeys(RedisType redisType, JedisCommands jedis, String keyPattern) {
        if (!redisType.equals(RedisType.CLUSTER)) {
            return ((Jedis) jedis).keys(keyPattern);
        }
        Set<String> keys = new TreeSet<>();
        Map<String, JedisPool> clusterNodes = ((JedisCluster) jedis).getClusterNodes();
        for (String k : clusterNodes.keySet()) {
            JedisPool jp = clusterNodes.get(k);
            try (Jedis connection = jp.getResource()) {
                keys.addAll(connection.keys(keyPattern));
            } catch (Exception e) {
                LOG.error("Getting keys error: ", e);
            }
        }
        return keys;
    }

    private GenericObjectPoolConfig setPoolConfig(String maxTotal, String maxIdle, String minIdle) {
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        if (Objects.nonNull(maxTotal)) {
            config.setMaxTotal(Integer.parseInt(maxTotal));
        }
        if (Objects.nonNull(maxIdle)) {
            config.setMaxIdle(Integer.parseInt(maxIdle));
        }
        if (Objects.nonNull(minIdle)) {
            config.setMinIdle(Integer.parseInt(minIdle));
        }
        return config;
    }
}
