package com.dtstack.flink.sql.side.rdb.async;

import com.dtstack.flink.sql.side.BaseAsyncReqRow;
import com.dtstack.flink.sql.side.BaseSideInfo;
import com.dtstack.flink.sql.side.rdb.testutil.ArgFactory;
import com.google.common.collect.Maps;
import io.vertx.core.json.JsonArray;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.powermock.api.support.membermodification.MemberModifier.suppress;

@RunWith(PowerMockRunner.class)
@PrepareForTest({BaseAsyncReqRow.class, ThreadPoolExecutor.class})
public class RdbAsyncReqRowTest {

    RdbAsyncReqRow reqRow;

    @Before
    public void setUp() {
        BaseSideInfo sideInfo = ArgFactory.getSideInfo();
        sideInfo.setOutFieldInfoList(
            ArgFactory.genOutFieldInfoList()
        );
        Map<Integer, Integer> inIndex = Maps.newHashMap();
        inIndex.put(0, 0);
        sideInfo.setInFieldIndex(inIndex);
        Map<Integer, String> sideIndex = Maps.newHashMap();
        sideIndex.put(1, "id");
        sideInfo.setSideFieldNameIndex(sideIndex);
        sideInfo.setRowTypeInfo(ArgFactory.genRowTypeInfo());
        reqRow = new RdbAsyncReqRow(sideInfo);
    }

    // @Test
    public void testOpen() throws Exception {
        suppress(BaseAsyncReqRow.class.getMethod("open", Configuration.class));
        reqRow.open(null);
    }

    @Test
    public void testFillData() {
        GenericRow input = new GenericRow(1);
        input.setField(0, 1);

        Map<String, Object> sideInput = Maps.newHashMap();
        sideInput.put("id", 1);
        reqRow.fillData(input, null);
        ArrayList<Object> ll = Lists.newArrayList();
        ll.add(1);
        ll.add("TEST_name");
        JsonArray jsonArray = new JsonArray(ll);
        reqRow.fillData(input, jsonArray);
    }

    // @Test
    public void testHandleAsyncInvoke() throws Exception {
        GenericRow input = new GenericRow(1);
        input.setField(0, 1);
        ResultFuture<BaseRow> future = new ResultFuture() {
            @Override
            public void complete(Collection result) { }

            @Override
            public void completeExceptionally(Throwable error) { }
        };
        Map<String, Object> sideInput = Maps.newHashMap();
        sideInput.put("id", 1);
        suppress(ThreadPoolExecutor.class.getMethod("execute", Runnable.class));
        suppress(BaseAsyncReqRow.class.getMethod("open", Configuration.class));
        reqRow.open(null);

        reqRow.handleAsyncInvoke(sideInput, input, future);
    }

    @Test
    public void testClose() throws Exception {
        reqRow.close();
    }

    @Test
    public void testConvertDataType() throws Exception {
        Timestamp ts = new Timestamp(1);
        String tsStr = Whitebox.invokeMethod(reqRow, "convertDataType", ts);
        Assert.assertEquals("1970-01-01 08:00:00", tsStr);
        Date date = new Date(1);
        LocalDate localDate = date.toLocalDate();
        String dateStr = Whitebox.invokeMethod(reqRow, "convertDataType", localDate);
        System.out.println(dateStr);
    }
}