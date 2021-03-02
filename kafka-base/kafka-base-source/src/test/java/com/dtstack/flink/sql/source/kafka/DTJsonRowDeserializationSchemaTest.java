package com.dtstack.flink.sql.source.kafka;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.json.DTJsonRowDeserializationSchema;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.*;

public class DTJsonRowDeserializationSchemaTest {

    @Test
    public void mockDeserialize() throws IOException {
        RowTypeInfo rowTypeInfo = mock(RowTypeInfo.class);
        when(rowTypeInfo.getFieldNames()).thenReturn(new String[]{"name"});
        when(rowTypeInfo.getFieldTypes()).thenReturn(new TypeInformation[]{Types.STRING});
        DTJsonRowDeserializationSchema dtJsonRowDeserializationSchema = new DTJsonRowDeserializationSchema(rowTypeInfo);

        DTJsonRowDeserializationSchema dtJsonRowDeserializationSchemaSpy = spy(dtJsonRowDeserializationSchema);
        String message = "{\"name\":\"roc\"}";
        dtJsonRowDeserializationSchemaSpy.deserialize(message.getBytes());
        verify(dtJsonRowDeserializationSchemaSpy).deserialize(message.getBytes());

    }
}
