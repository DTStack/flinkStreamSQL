package com.dtstack.flink.sql.side.oracle;

import com.dtstack.flink.sql.side.rdb.table.RdbSideTableInfo;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import org.junit.Assert;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

public class OracleAsyncSideInfoTest {

    @Test
    public void testWrapperPlaceholder() {
        RdbSideTableInfo tableInfo = new RdbSideTableInfo();
        String fieldName = "TEST_name";
        String type = "char";
        tableInfo.addField(fieldName);
        tableInfo.addFieldType(type);

        AbstractTableInfo.FieldExtraInfo extraInfo = new AbstractTableInfo.FieldExtraInfo();
        extraInfo.setLength(4);
        tableInfo.addFieldExtraInfo(extraInfo);

        OracleAsyncSideInfo sideInfo = Whitebox.newInstance(OracleAsyncSideInfo.class);
        Whitebox.setInternalState(sideInfo, "sideTableInfo", tableInfo);

        String placeholder = sideInfo.wrapperPlaceholder(fieldName);
        Assert.assertEquals("rpad(?, 4, ' ')", placeholder);
    }

}