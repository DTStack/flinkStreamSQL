package com.dtstack.flink.sql.side.rdb.async;

import com.dtstack.flink.sql.side.BaseSideInfo;
import com.dtstack.flink.sql.side.rdb.all.AbstractRdbAllReqRow;
import com.dtstack.flink.sql.side.rdb.table.RdbSideParser;
import com.dtstack.flink.sql.side.rdb.table.RdbSideTableInfo;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLClient;
import org.apache.flink.configuration.Configuration;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.HashMap;
import java.util.Map;

import static org.powermock.api.support.membermodification.MemberModifier.suppress;

/**
 * @program: flinkStreamSQL
 * @author: wuren
 * @create: 2020/11/10
 **/
@RunWith(PowerMockRunner.class)
@PrepareForTest({RdbAsyncReqRow.class, JDBCClient.class})
public abstract class RdbAsyncReqRowTestBase {

    protected RdbAsyncReqRow reqRow;

    protected Class<? extends RdbAsyncReqRow> clazz;

    @Before
    public void setUp() {
        init();
        this.reqRow = Whitebox.newInstance(clazz);
    }

    protected abstract void init();

    @BeforeClass
    public static void perpare() throws NoSuchMethodException {
        suppress(RdbAsyncReqRow.class.getMethod("buildJdbcConfig"));
        suppress(RdbAsyncReqRow.class.getMethod("open", Configuration.class));
        suppress(JDBCClient.class.getMethod("createNonShared", Vertx.class, JsonObject.class));
    }

    @Test
    public void testOpen() throws Exception {
        final String tableName = "table_foo";
        final String fieldsInfo = "id INT, name VARCHAR, PRIMARY  KEY  (id)  , PERIOD  FOR  SYSTEM_TIME";
        Map<String, Object> props = new HashMap<String, Object>();
        props.put("url", "jdbc:mysql://foo.com:21050");
        props.put("tablename", "table_foo");
        props.put("username", "foo");
        props.put("password", "foo");
        RdbSideParser sinkParser = new RdbSideParser();
        RdbSideTableInfo tableInfo = (RdbSideTableInfo) sinkParser.getTableInfo(tableName, fieldsInfo, props);
        tableInfo.setAsyncPoolSize(1);
        BaseSideInfo sideInfo = Whitebox.newInstance(RdbAsyncSideInfo.class);
        Whitebox.setInternalState(sideInfo, "sideTableInfo", tableInfo);
        Whitebox.setInternalState(reqRow, "sideInfo", sideInfo);
        reqRow.open(null);
    }

}
