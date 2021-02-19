package com.dtstack.flink.sql.sink.kafka.serialization;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.types.Row;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;

public class JsonTuple2SerializationSchemaTest {

    @Test
    public void serialize(){
        List<String> fieldNames = Lists.newArrayList("a","b","c","d","e","f","g","h","i","j","k","l","m");
        List<TypeInformation<?>> information = Lists.newArrayList(Types.VOID, Types.BOOLEAN, Types.STRING, Types.BIG_DEC,
                Types.BIG_INT,Types.SQL_DATE,Types.SQL_TIME, Types.SQL_TIMESTAMP, Types.ROW_NAMED(new String[]{"a"},
                        Types.STRING), Types.OBJECT_ARRAY(Types.BOOLEAN), Types.OBJECT_ARRAY(Types.STRING),
                Types.PRIMITIVE_ARRAY(Types.BOOLEAN), Types.INSTANT);
        TypeInformation typeInformation = new TupleTypeInfo(org.apache.flink.table.api.Types.BOOLEAN(), new RowTypeInfo(information.toArray(new TypeInformation[13]), fieldNames.toArray(new String[13])));
        JsonTupleSerializationSchema jsonCRowSerializationSchema = new JsonTupleSerializationSchema(typeInformation, "upsert");
        Row row = new Row(13);
        row.setField(0, "a");
        row.setField(1, true);
        row.setField(2, "str");
        row.setField(3, new BigDecimal("1"));
        row.setField(4, new BigInteger("1"));
        row.setField(5, "2020");
        row.setField(6, new Time(System.currentTimeMillis()));
        row.setField(7, new Timestamp(System.currentTimeMillis()));
        Row rowNest = new Row(1);
        rowNest.setField(0, "a");
        row.setField(8,  rowNest);
        row.setField(9, new Boolean[]{false});
        row.setField(10, new String[]{"a"});
        row.setField(11, new byte[]{1});
        row.setField(12, "");
        jsonCRowSerializationSchema.serialize(new Tuple2<>(true, row));
    }
}
