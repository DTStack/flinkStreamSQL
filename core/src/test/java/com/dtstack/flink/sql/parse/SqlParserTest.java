package com.dtstack.flink.sql.parse;

import com.dtstack.flink.sql.parser.CreateTableParser;
import com.dtstack.flink.sql.parser.CreateTmpTableParser;
import com.dtstack.flink.sql.parser.InsertSqlParser;
import com.dtstack.flink.sql.parser.SqlParser;
import com.dtstack.flink.sql.parser.SqlTree;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.table.AbstractTableInfoParser;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Map;

import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyObject;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({SqlParser.class, SqlTree.class})
public class SqlParserTest {
    @Test
    public void parseSql() throws Exception {

        SqlParser.setLocalSqlPluginRoot("/");
        String sql = "CREATE TABLE MyTable(\n" +
                "    name varchar,\n" +
                "    channel varchar\n" +
                " )WITH(\n" +
                "    type ='kafka10',\n" +
                "    bootstrapServers ='172.16.8.107:9092',\n" +
                "    zookeeperQuorum ='172.16.8.107:2181/kafka',\n" +
                "    offsetReset ='latest',\n" +
                "    topic ='mqTest01',\n" +
                "    timezone='Asia/Shanghai',\n" +
                "    updateMode ='append',\n" +
                "    enableKeyPartitions ='false',\n" +
                "    topicIsPattern ='false',\n" +
                "    parallelism ='1'\n" +
                " );\n" +
                "\n" +
                "CREATE TABLE sideTable(\n" +
                "    cf:name varchar as name,\n" +
                "    cf:info varchar as info,\n" +
                "    PRIMARY KEY(rowkey) ,\n" +
                "    PERIOD FOR SYSTEM_TIME\n" +
                " )WITH(\n" +
                "    type ='hbase',\n" +
                "    zookeeperQuorum ='172.16.10.85:2181,172.16.10.228:2181,172.16.10.235:2181',\n" +
                "    zookeeperParent ='/hbase',\n" +
                "    tableName ='workerinfo',\n" +
                "    partitionedJoin ='false',\n" +
                "    cache ='LRU',\n" +
                "    cacheSize ='10000',\n" +
                "    cacheTTLMs ='60000',\n" +
                "    parallelism ='1',\n" +
                "    hbase.security.auth.enable='true',\n" +
                "    hbase.security.authentication='kerberos',\n" +
                "    hbase.sasl.clientconfig='Client',\n" +
                "    hbase.kerberos.regionserver.principal='hbase/_HOST@DTSTACK.COM',\n" +
                "    hbase.keytab='yijing.keytab',\n" +
                "    hbase.principal='yijing@DTSTACK.COM',\n" +
                "    java.security.krb5.conf='krb5.conf'\n" +
                " );\n" +
                "\n" +
                " CREATE TABLE MyResult(\n" +
                "    pv VARCHAR,\n" +
                "    channel VARCHAR\n" +
                " )WITH(\n" +
                "    type ='mysql',\n" +
                "    url ='jdbc:mysql://172.16.101.249:3306/roc',\n" +
                "    userName ='drpeco',\n" +
                "    password ='DT@Stack#123',\n" +
                "    tableName ='myresult',\n" +
                "    updateMode ='append',\n" +
                "    parallelism ='1',\n" +
                "    batchSize ='100',\n" +
                "    batchWaitInterval ='1000'\n" +
                " );";
        AbstractTableInfoParser tableInfoParser = mock(AbstractTableInfoParser.class);
        PowerMockito.whenNew(AbstractTableInfoParser.class).withAnyArguments().thenReturn(tableInfoParser);
        AbstractTableInfo tableInfo = mock(AbstractTableInfo.class);
        when(tableInfoParser.parseWithTableType(anyInt(), anyObject(), anyString(), anyString())).thenReturn(tableInfo);

        SqlTree sqlTree = mock(SqlTree.class);
        PowerMockito.whenNew(SqlTree.class).withAnyArguments().thenReturn(sqlTree);

        InsertSqlParser.SqlParseResult sqlParseResult = mock(InsertSqlParser.SqlParseResult.class);
        when(sqlParseResult.getSourceTableList()).thenReturn(Lists.newArrayList("kafka"));
        when(sqlParseResult.getTargetTableList()).thenReturn(Lists.newArrayList("mysql"));

        when(sqlTree.getExecSqlList()).thenReturn(Lists.newArrayList(sqlParseResult));
        CreateTableParser.SqlParserResult sqlParserResult = mock(CreateTableParser.SqlParserResult.class);
        Map preDealTable = Maps.newHashMap();
        preDealTable.put("kafka", sqlParserResult);
        preDealTable.put("mysql", sqlParserResult);
        when(sqlTree.getPreDealTableMap()).thenReturn(preDealTable);


        CreateTmpTableParser.SqlParserResult tmpSqlParserResult = mock(CreateTmpTableParser.SqlParserResult.class);
        when(sqlTree.getTmpSqlList()).thenReturn(Lists.newArrayList(tmpSqlParserResult));
        when(tmpSqlParserResult.getSourceTableList()).thenReturn(Lists.newArrayList("kafka"));

        SqlParser.parseSql(sql, "");
    }
}
