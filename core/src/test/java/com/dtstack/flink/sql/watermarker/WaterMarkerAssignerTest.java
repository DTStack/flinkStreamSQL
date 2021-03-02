package com.dtstack.flink.sql.watermarker;

import com.dtstack.flink.sql.table.AbstractSourceTableInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WaterMarkerAssignerTest {

    @Test
    public void checkNeedAssignWaterMarker(){
        WaterMarkerAssigner waterMarkerAssigner = new WaterMarkerAssigner();
        AbstractSourceTableInfo sourceTableInfo = mock(AbstractSourceTableInfo.class);
        when(sourceTableInfo.getEventTimeField()).thenReturn("ss");
        Assert.assertEquals(waterMarkerAssigner.checkNeedAssignWaterMarker(sourceTableInfo), true);

        when(sourceTableInfo.getEventTimeField()).thenReturn("");
        Assert.assertEquals(waterMarkerAssigner.checkNeedAssignWaterMarker(sourceTableInfo), false);
    }

    @Test
    public void assignWaterMarker(){
        WaterMarkerAssigner waterMarkerAssigner = new WaterMarkerAssigner();
        AbstractSourceTableInfo sourceTableInfo = mock(AbstractSourceTableInfo.class);
        DataStream<Row> dataStream = mock(DataStream.class);
        RowTypeInfo typeInfo = mock(RowTypeInfo.class);

        when(sourceTableInfo.getEventTimeField()).thenReturn("aa");
        when(sourceTableInfo.getMaxOutOrderness()).thenReturn(1);
        when(sourceTableInfo.getName()).thenReturn("roc");
        when(sourceTableInfo.getTimeZone()).thenReturn("CST");
        String[] fieldNames = new String[1];
        fieldNames[0] = "aa";

        TypeInformation typeInformation = mock(TypeInformation.class);
        when(typeInformation.getTypeClass()).thenReturn(Long.class);
        TypeInformation<?>[] fieldTypes = new TypeInformation[1];
        fieldTypes[0] = typeInformation;
        when(typeInfo.getFieldNames()).thenReturn(fieldNames);
        when(typeInfo.getFieldTypes()).thenReturn(fieldTypes);

        waterMarkerAssigner.assignWaterMarker(dataStream, typeInfo, sourceTableInfo);
    }
}
