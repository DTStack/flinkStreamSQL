package com.dtstack.flink.sql.side.rdb.provider;

import com.google.common.collect.Maps;
import io.vertx.core.json.JsonObject;
import org.junit.Test;

import javax.sql.DataSource;

import java.sql.SQLException;
import java.util.Map;

public class DTC3P0DataSourceProviderTest {

    private static String TEST_STR = "TEST";
    private static int TEST_INT = 1;
    private static boolean TEST_BOOLEAN = false;

    @Test
    public void testGetDataSource() throws SQLException {
        Map<String, Object> map = Maps.newHashMap();
        DTC3P0DataSourceProvider provider = new DTC3P0DataSourceProvider();
        try {
            JsonObject json = new JsonObject(map);
            provider.getDataSource(json);
        } catch (NullPointerException e) { }
        map.put("url", "jdbc://");
        map.put("driver_class", "com.mysql.jdbc.Driver");
        map.put("user", TEST_STR);
        map.put("password", TEST_STR);
        map.put("max_pool_size", TEST_INT);
        map.put("initial_pool_size", TEST_INT);
        map.put("min_pool_size", TEST_INT);
        map.put("max_statements", TEST_INT);
        map.put("max_statements_per_connection", TEST_INT);
        map.put("max_idle_time", TEST_INT);
        map.put("acquire_retry_attempts", TEST_INT);
        map.put("acquire_retry_delay", TEST_INT);
        map.put("break_after_acquire_failure", TEST_BOOLEAN);

        map.put("preferred_test_query", TEST_STR);
        map.put("idle_connection_test_period", TEST_INT);
        map.put("test_connection_on_checkin", TEST_BOOLEAN);

        JsonObject json = new JsonObject(map);
        DataSource ds = provider.getDataSource(json);
    }

}