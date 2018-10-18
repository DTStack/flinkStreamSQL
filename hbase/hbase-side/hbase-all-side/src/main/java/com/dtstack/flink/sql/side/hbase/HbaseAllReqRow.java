package com.dtstack.flink.sql.side.hbase;

import com.dtstack.flink.sql.side.*;
import com.dtstack.flink.sql.side.hbase.table.HbaseSideTableInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class HbaseAllReqRow extends AllReqRow {

    private static final Logger LOG = LoggerFactory.getLogger(HbaseAllReqRow.class);

    private String tableName;

    private AtomicReference<Map<String, Map<String, Object>>> cacheRef = new AtomicReference<>();

    public HbaseAllReqRow(RowTypeInfo rowTypeInfo, JoinInfo joinInfo, List<FieldInfo> outFieldInfoList, SideTableInfo sideTableInfo) {
        super(new HbaseAllSideInfo(rowTypeInfo, joinInfo, outFieldInfoList, sideTableInfo));
        tableName = ((HbaseSideTableInfo)sideTableInfo).getTableName();
    }

    @Override
    protected Row fillData(Row input, Object sideInput) {
        Map<String, Object> sideInputList = (Map<String, Object>) sideInput;
        Row row = new Row(sideInfo.getOutFieldInfoList().size());
        for(Map.Entry<Integer, Integer> entry : sideInfo.getInFieldIndex().entrySet()){
            Object obj = input.getField(entry.getValue());
            boolean isTimeIndicatorTypeInfo = TimeIndicatorTypeInfo.class.isAssignableFrom(sideInfo.getRowTypeInfo().getTypeAt(entry.getValue()).getClass());

            //Type information for indicating event or processing time. However, it behaves like a regular SQL timestamp but is serialized as Long.
            if(obj instanceof Timestamp && isTimeIndicatorTypeInfo){
                obj = ((Timestamp)obj).getTime();
            }
            row.setField(entry.getKey(), obj);
        }

        for(Map.Entry<Integer, Integer> entry : sideInfo.getSideFieldIndex().entrySet()){
            if(sideInputList == null){
                row.setField(entry.getKey(), null);
            }else{
                row.setField(entry.getKey(), sideInputList.get(entry.getValue()));
            }
        }

        return row;
    }

    @Override
    protected void initCache() throws SQLException {
        Map<String, Map<String, Object>> newCache = Maps.newConcurrentMap();
        cacheRef.set(newCache);
        loadData(newCache);
    }

    @Override
    protected void reloadCache() {
        Map<String, Map<String, Object>> newCache = Maps.newConcurrentMap();
        try {
            loadData(newCache);
        } catch (SQLException e) {
            LOG.error("", e);
        }

        cacheRef.set(newCache);
        LOG.info("----- HBase all cacheRef reload end:{}", Calendar.getInstance());
    }

    @Override
    public void flatMap(Row value, Collector<Row> out) throws Exception {
        Map<String, Object> refData = Maps.newHashMap();
        for (int i = 0; i < sideInfo.getEqualValIndex().size(); i++) {
            Integer conValIndex = sideInfo.getEqualValIndex().get(i);
            Object equalObj = value.getField(conValIndex);
            if(equalObj == null){
                out.collect(null);
            }
            refData.put(sideInfo.getEqualFieldList().get(i), equalObj);
        }

        String rowKeyStr = ((HbaseAllSideInfo)sideInfo).getRowKeyBuilder().getRowKey(refData);

        Object cacheList = cacheRef.get().get(rowKeyStr);
        Row row = fillData(value, cacheList);
        out.collect(row);
    }

    private void loadData(Map<String, Map<String, Object>> tmpCache) throws SQLException {
        SideTableInfo sideTableInfo = sideInfo.getSideTableInfo();
        HbaseSideTableInfo hbaseSideTableInfo = (HbaseSideTableInfo) sideTableInfo;
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", hbaseSideTableInfo.getHost());
        Connection conn = null;
        Table table = null;
        ResultScanner resultScanner = null;
        try {
            conn = ConnectionFactory.createConnection(conf);
            table = conn.getTable(TableName.valueOf(tableName));
            resultScanner = table.getScanner(new Scan());
            List<String> rows = new LinkedList<>();
            for (Result r : resultScanner) {
                for (Cell cell : r.listCells()){
                    rows.add(cell.getRow().toString());
                }
            }
            //根据表，rowkey查询值
            for (int i=0; i < rows.size(); i++){
                Get get = new Get(Bytes.toBytes(rows.get(i)));
                Result result = table.get(get);
                tmpCache.put(rows.get(i), result2Map(result));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                conn.close();
                table.close();
                resultScanner.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static Map<String, Object> result2Map(Result result) {
        Map<String, Object> ret = new HashMap<String, Object>();
        if (result != null && result.listCells() != null) {
            for (Cell cell : result.listCells()) {
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                StringBuilder key = new StringBuilder();
                key.append(family).append(":").append(qualifier);
                ret.put(key.toString(), value);
            }
        }
        return ret;
    }
}
