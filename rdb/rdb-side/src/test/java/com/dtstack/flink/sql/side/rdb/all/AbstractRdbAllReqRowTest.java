package com.dtstack.flink.sql.side.rdb.all;

import com.dtstack.flink.sql.side.BaseAllReqRow;
import com.dtstack.flink.sql.side.rdb.table.RdbSideTableInfo;
import com.dtstack.flink.sql.side.rdb.testutil.ArgFactory;
import com.google.common.collect.Maps;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.List;
import java.util.Map;

import static org.powermock.api.support.membermodification.MemberModifier.suppress;

@RunWith(PowerMockRunner.class)
@PrepareForTest({AbstractRdbAllReqRow.class, BaseAllReqRow.class})
public class AbstractRdbAllReqRowTest {

    AbstractRdbAllReqRow reqRow;

    @Before
    public void setUp() {
        RdbAllSideInfo sideInfo = Whitebox.newInstance(RdbAllSideInfo.class);
        RdbSideTableInfo sideTableInfo = new RdbSideTableInfo();
        sideInfo.setSideTableInfo(sideTableInfo);

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

        sideInfo.setSideSelectFields("id, name");
        sideInfo.setSideTableInfo(
            ArgFactory.genSideTableInfo()
        );
        List<Integer> equalIndex = Lists.newArrayList();
        equalIndex.add(0);
        sideInfo.setEqualValIndex(equalIndex);
        List<String> equalList = Lists.newArrayList();
        equalList.add("id");
        sideInfo.setEqualFieldList(equalList);
        reqRow = new ConcreteRdbAllReqRow(sideInfo);
    }

//    @Test
    public void testOpen() throws Exception {
        suppress(
            BaseAllReqRow.class.getMethod( "open", Configuration.class)
        );
        TypeInformation<?>[] types = new TypeInformation[] {
            Types.INT(),
            Types.STRING()
        };
        String[] fieldNames = new String[] {
            "id",
            "name"
        };
        RowTypeInfo rowTypeInfo = new RowTypeInfo(types, fieldNames);
        reqRow.open(null);
    }

//    @Test
//    public void testInitCache() throws Exception {
//            Whitebox.invokeMethod(reqRow, "initCache");
//    }
//
//    @Test
//    public void testReloadCache() throws Exception {
//        Whitebox.invokeMethod(reqRow, "reloadCache");
//    }

//    @Test
//    public void testFlatMap() throws Exception {
//        Whitebox.invokeMethod(reqRow, "initCache");
//
//        Row input = new Row(1);
//        input.setField(0, 1);
//        Collector<BaseRow> out = new Collector<BaseRow>() {
//            @Override
//            public void collect(BaseRow record) { }
//
//            @Override
//            public void close() { }
//        };
//
//        reqRow.flatMap(input, out);
//    }

//    @Test
    public void testFillData() {
        GenericRow input = new GenericRow(1);
        input.setField(0, 1);

        Map<String, Object> sideInput = Maps.newHashMap();
        sideInput.put("id", 1);
        reqRow.fillData(input, sideInput);
    }

}