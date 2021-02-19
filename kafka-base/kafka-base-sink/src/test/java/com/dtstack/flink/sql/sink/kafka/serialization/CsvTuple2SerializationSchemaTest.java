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

public class CsvTuple2SerializationSchemaTest {
    @Test
    public void serialize(){
        List<String> fieldNames = Lists.newArrayList("a","b","c","d","e","f","g","h","i","j","k","l","m","n","o","p","q");
        List<TypeInformation<?>> information = Lists.newArrayList(Types.VOID, Types.STRING, Types.BOOLEAN, Types.BYTE,
                Types.SHORT,Types.INT,Types.LONG,Types.FLOAT, Types.DOUBLE, Types.BIG_DEC, Types.BIG_INT,
                Types.SQL_DATE,Types.SQL_TIME, Types.SQL_TIMESTAMP, Types.ROW_NAMED(new String[]{"a"}, Types.STRING),
                Types.OBJECT_ARRAY(Types.BOOLEAN), Types.OBJECT_ARRAY(Types.STRING));
        TypeInformation<Tuple2<Boolean, Row>> typeInformation =
                new TupleTypeInfo<>(org.apache.flink.table.api.Types.BOOLEAN(), new RowTypeInfo(information.toArray(new TypeInformation[13]), fieldNames.toArray(new String[13])));


        CsvTupleSerializationSchema csvTupleSerializationSchema = new CsvTupleSerializationSchema.Builder(typeInformation)
                .setFieldDelimiter(',')
                .setUpdateMode("upsert")
                .setArrayElementDelimiter(",")
                .setEscapeCharacter(',')
                .setLineDelimiter("\n")
                .setNullLiteral("")
                .setQuoteCharacter('\'')
                .build();
        Row row = new Row(17);
        row.setField(0, "a");
        row.setField(1, "s");
        row.setField(2, true);
        row.setField(3,  Byte.valueOf("1"));
        row.setField(4, new Short("1"));
        row.setField(5, 1);
        row.setField(6, 1L);
        row.setField(7, 1f);
        row.setField(8, 1d);
        row.setField(9, new BigDecimal("1"));
        row.setField(10, new BigInteger("1"));
        row.setField(11, "2020");
        row.setField(12,  new Time(System.currentTimeMillis()));
        row.setField(13, new Timestamp(System.currentTimeMillis()));
        Row rowNest = new Row(1);
        rowNest.setField(0, "a");
        row.setField(14,  rowNest);
        row.setField(15, new Boolean[]{false});
        row.setField(16, new String[]{"a"});
        csvTupleSerializationSchema.serialize(new Tuple2<>(false, row));

    }
}
