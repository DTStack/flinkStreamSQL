/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.dtstack.flink.sql.side;

import com.dtstack.flink.sql.Main;
import com.dtstack.flink.sql.parser.SqlParser;
import com.dtstack.flink.sql.parser.SqlTree;
import org.apache.flink.calcite.shaded.com.google.common.base.Charsets;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.List;

/**
 * Reason:
 * Date: 2018/7/24
 * Company: www.dtstack.com
 *
 * @author xuchao
 */

public class SideSqlExecTest {

    /**
     * 参考阿里Blink的cep sql语法，文档https://help.aliyun.com/document_detail/73845.html?spm=a2c4g.11186623.6.637.5cba27efFHjOSs
     * @throws Exception
     */
    @Test
    public void testCepSql() throws Exception {
        List<String> paramList = Lists.newArrayList();
        paramList.add("-sql");
        String sqlContext = "CREATE table source(" +
                "name varchar, " +
                "price float, " +
                "tax float, " +
                "tstamp timestamp) " +
                "with (" +
                " type = 'kafka09',bootstrapServers = 'kudu1:9092',zookeeperQuorum = '172.16.8.107:2181/kafka', offsetReset = 'latest',topic = 'tranflow_input',parallelism = '1' " +
                ");"
                + "CREATE table sink(" +
                "start_tstamp timestamp, " +
                "bottom_tstamp timestamp, " +
                "end_tstamp timestamp, " +
                "bottom_total float, " +
                "end_total float ) " +
                "with (" +
                " type = 'mysql',url = 'jdbc:mysql://172.16.8.104:3306/bank_test?charset=utf8',userName = 'dtstack',password = 'abc123',tableName = 'max_deposit_acct_base',cache = 'LRU',cacheSize = '10000',cacheTTLMs = '60000',parallelism = '1' " +
                ");"
                + "insert into sink " +
                "select * from source " +
                "MATCH_RECOGNIZE (\n" +
                "  MEASURES\n" +
                "    STRT.tstamp AS start_tstamp,\n" +
                "    LAST(DOWN.tstamp) AS bottom_tstamp,\n" +
                "    LAST(UP.tstamp) AS end_tstamp,\n" +
                "    FIRST(DOWN.price + DOWN.tax + 1) AS bottom_total,\n" +
                "    FIRST(UP.price + UP.tax) AS end_total" +
                "  ONE ROW PER MATCH\n" +
                "  PATTERN (STRT DOWN+ UP+)\n" +
                "  DEFINE\n" +
                "    DOWN AS DOWN.price < PREV(DOWN.price),\n" +
                "    UP AS UP.price > PREV(UP.price) AND UP.tax > LAST(DOWN.tax)\n" +
                ") AS T"
                ;
        test(sqlContext);
    }


    @Test
    public void testRunSideSql() throws Exception {
        //String runParam = "-sql CREATE+TABLE+MyTable(channel+STRING%2c+pv+INT%2c+xctime+bigint%2c+timeLeng+as+CHARACTER_LENGTH(channel)%2c++WATERMARK+FOR+xctime+AS+withOffset(xctime%2c+1000))+WITH+(+type%3d%27kafka09%27%2c+bootstrapServers%3d%27172.16.8.198%3a9092%27%2c+offsetReset%3d%27latest%27%2ctopic%3d%27nbTest1%27)%3bCREATE+TABLE+MyResult(channel+STRING%2c+pv+INT)+WITH+(+type%3d%27mysql%27%2c+url%3d%27jdbc%3amysql%3a%2f%2f172.16.8.104%3a3306%2ftest%3fcharset%3dutf8%27%2cuserName%3d%27dtstack%27%2cpassword%3d%27abc123%27%2c+tableName%3d%27pv%27)%3bcreate+table+sideTable(channel+String%2c+count+int%2c+PERIOD+FOR+SYSTEM_TIME)+WITH+(+type%3d%27mysql%27%2c+url%3d%27jdbc%3amysql%3a%2f%2f172.16.8.104%3a3306%2ftest%3fcharset%3dutf8%27%2cuserName%3d%27dtstack%27%2cpassword%3d%27abc123%27%2c+tableName%3d%27pv%27)%3binsert+into+MyResult+select+a.channel%2cb.pv+from+MyTable+a+join+sideTable+b+on+a.channel%3db.channel%3b -name xc -localSqlPluginPath D:\\gitspace\\flink-sql-plugin\\plugins -mode local -remoteSqlPluginPath /opt/dtstack/flinkplugin -confProp %7b%22time.characteristic%22%3a%22EventTime%22%7d -addjar %5b%22D%3a%5c%5cgitspace%5c%5crdos-execution-engine%5c%5c..%5c%5ctmp140%5c%5cflink14Test-1.0-SNAPSHOT.jar%22%5d";

        List<String> paramList = Lists.newArrayList();
        paramList.add("-sql");
//        String sql = "CREATE TABLE MyTable(channel STRING, pv INT, xctime bigint, timeLeng as CHARACTER_LENGTH(channel),  WATERMARK FOR xctime AS withOffset(xctime, 1000)) WITH ( type='kafka09', bootstrapServers='172.16.8.198:9092', offsetReset='latest',topic='nbTest1');" +
//                "CREATE TABLE MyResult(channel STRING, pv INT) WITH ( type='mysql', url='jdbc:mysql://172.16.8.104:3306/test?charset=utf8',userName='dtstack',password='abc123', tableName='pv');" +
//               "create table sideTable(channel String, xccount int, PRIMARY KEY (channel),PERIOD FOR SYSTEM_TIME) WITH ( type='mysql', url='jdbc:mysql://172.16.8.104:3306/test?charset=utf8',userName='dtstack',password='abc123', tableName='sidetest');" +
//                "insert into MyResult select a.channel,b.xccount from MyTable a join sideTable b on a.channel=b.channel where b.channel = 'xc' and a.pv=10";

        //String insetSql = "insert into MyResult select a.channel,b.xccount from MyTable a join sideTable b on a.channel=b.channel where b.channel = 'xc' and a.pv=10;";
        //String insetSql = "insert into MyResult select a.channel,b.xccount from (select * from MyTable) a left join sideTable b on a.channel=b.channel where a.channel = 'xc1' and a.pv=10;";
        //String insetSql = "insert into MyResult select * from sideTable";
        //sql = sql + insetSql;
        //String sql = "create scala function xchashcode with com.xc.udf.MyHashCode;CREATE TABLE MyTable ( channel STRING, pv INT, xctime bigint, timeLeng AS CHARACTER_LENGTH(channel),WATERMARK FOR  xctime AS withOffset( xctime , 10000 )) WITH (type = 'kafka09',bootstrapServers = '172.16.8.198:9092',zookeeperConnect = '172.16.8.198:2181/kafka', offsetReset = 'latest',topic = 'nbTest1',parallelism = '1');CREATE TABLE MyResult ( channel VARCHAR, pv bigint) WITH (type = 'mysql',url = 'jdbc:mysql://172.16.8.104:3306/test?charset=utf8',userName = 'dtstack',password = 'abc123',tableName = 'pv',parallelism = '1');insert  into MyResult select channel, count(channel)  from  MyTable  GROUP BY  channel ;";
        //String sql = "create scala function xchashcode with com.xc.udf.MyHashCode;CREATE TABLE MyTable ( channel STRING, pv INT, xctime bigint, timeLeng AS CHARACTER_LENGTH(channel),WATERMARK FOR  xctime AS withOffset( xctime , 10000 )) WITH (type = 'kafka09',bootstrapServers = '172.16.8.198:9092',zookeeperConnect = '172.16.8.198:2181/kafka', offsetReset = 'latest',topic = 'nbTest1',parallelism = '1');CREATE TABLE MyResult ( channel VARCHAR, pv bigint) WITH (type = 'mysql',url = 'jdbc:mysql://172.16.8.104:3306/test?charset=utf8',userName = 'dtstack',password = 'abc123',tableName = 'pv',parallelism = '1');insert  into MyResult select channel, count(channel)  from  MyTable  GROUP BY  TUMBLE(rowtime, INTERVAL '3' SECOND), channel ;";
//        String sql = "CREATE TABLE MyTable(channel STRING, pv INT, xctime bigint, timeLeng as CHARACTER_LENGTH(channel),  WATERMARK FOR xctime AS withOffset(xctime, 100)) WITH ( type='kafka09', bootstrapServers='172.16.8.198:9092', offsetReset='latest',topic='nbTest1');\n" +
//                "CREATE TABLE MyResult(cf:channel STRING, cf:pv BIGINT) WITH ( type='hbase', zookeeperQuorum='rdos1:2181', zookeeperParent = '/hbase', tableName='tb1', rowkey='cf:channel');\n" +
//                "insert into MyResult select channel from MyTable group by channel";
        //String sql ="CREATE TABLE student_1 ( id_1 varchar, name_1 varchar, sex_1 varchar) WITH (type = 'kafka09',bootstrapServers = '172.16.8.198:9092',zookeeperQuorum = '172.16.8.198:2181/kafka', offsetReset = 'latest',topic = 'test',parallelism = '1');CREATE TABLE sum_1 ( id_1 VARCHAR, sex_1 VARCHAR, name_1 VARCHAR, mark_1 INT) WITH (type = 'mysql',url = 'jdbc:mysql://172.16.8.104:3306/test?charset=utf8',userName = 'dtstack',password = 'abc123',tableName = 'sum_1',parallelism = '1');CREATE TABLE score_1 ( id_1 VARCHAR, name_1 VARCHAR, mark_1 INT,PRIMARY KEY (id_1) , PERIOD FOR SYSTEM_TIME ) WITH (type = 'mysql',url = 'jdbc:mysql://172.16.8.104:3306/test?charset=utf8',userName = 'dtstack',password = 'abc123',tableName = 'score_1',cache = 'LRU',cacheSize = '10000',cacheTTLMs = '60000',parallelism = '1');insert  into    sum_1  select    a.id_1,     a.sex_1,     a.name_1,     b.mark_1  from       student_1 a    inner join    score_1 b  on a.id=b.id_1;";
        //String sql = "CREATE TABLE MyTable ( channel STRING, pv INT, xctime bigint) WITH (type = 'kafka09',bootstrapServers = '172.16.8.198:9092',zookeeperQuorum = '172.16.8.198:2181/kafka', offsetReset = 'latest',topic = 'nbTest1',parallelism = '1');CREATE TABLE MyResult ( aa INT, bb INT) WITH (type = 'elasticsearch',address = '172.16.10.47:9500',cluster='es_47_menghan',estype = 'type1',index = 'xc_es_test',id = '0,1',parallelism = '1');insert into MyResult select pv,pv from MyTable;";

        String sql = "CREATE TABLE bal_cur_batch_s30_pb_tranflow_input (trf_flowno varchar, trf_batno varchar, trf_cstno varchar, trf_bsncode varchar, trf_transtype varchar, trf_payacc varchar, trf_paysubacc varchar, trf_payname varchar, trf_rcvacc varchar, trf_rcvsubacc varchar, trf_rcvname varchar, trf_rcvbank varchar, trf_comitrno varchar, trf_crytype varchar, trf_subtime varchar, trf_tranamet varchar, trf_fee1 varchar, trf_fee2 varchar, trf_fee3 varchar, trf_userrem varchar, trf_hostflwno varchar, trf_hostsendtime varchar, trf_hosterror varchar, trf_lastupdtime varchar, trf_stt varchar, trf_schl_flowno varchar, trf_logontype varchar, trf_reccity varchar, trf_recprovice varchar, trf_channel varchar, trf_hosterrormsg varchar, trf_ext1 varchar, trf_ext2 varchar, trf_security varchar, cast(current_timestamp as varchar) as currTime) WITH (type = 'kafka09',bootstrapServers = 'kudu1:9092',zookeeperQuorum = '172.16.8.107:2181/kafka', offsetReset = 'latest',topic = 'tranflow_input',parallelism = '1');CREATE TABLE resultbank ( run_date VARCHAR, run_time VARCHAR, trf_flowno VARCHAR, trf_payname VARCHAR, trf_payacc VARCHAR, trf_tranamet VARCHAR, trf_subtime VARCHAR, trf_rcvbank VARCHAR,PRIMARY KEY (run_date,run_time,trf_flowno) ) WITH (type = 'mysql',url = 'jdbc:mysql://172.16.8.104:3306/bank_test?charset=utf8',userName = 'dtstack',password = 'abc123',tableName = 'resultbank',parallelism = '1');CREATE TABLE bal_cur_batch_rds_report_lrlct_trans_cur_input ( run_date VARCHAR, run_time VARCHAR, trf_flowno VARCHAR, trf_payname VARCHAR, trf_payacc VARCHAR, trf_subtime VARCHAR, trf_rcvbank VARCHAR,PRIMARY KEY (run_date,trf_flowno) , PERIOD FOR SYSTEM_TIME ) WITH (type = 'mysql',url = 'jdbc:mysql://172.16.8.104:3306/bank_test?charset=utf8',userName = 'dtstack',password = 'abc123',tableName = 'rds_report_lrlc_trans_cur',cache = 'LRU',cacheSize = '10000',cacheTTLMs = '60000',parallelism = '1');CREATE TABLE bal_cur_batch_t03_deposit_acct_base_info_cur_input ( data_dt VARCHAR, card_id VARCHAR, sub_acct_id VARCHAR, acct_org varchar, acct_stat_cd VARCHAR, core_cust_id VARCHAR, cust_rm VARCHAR, cust_scale_cd VARCHAR, item_id VARCHAR,PRIMARY KEY (card_id) , PERIOD FOR SYSTEM_TIME ) WITH (type = 'mysql',url = 'jdbc:mysql://172.16.8.104:3306/bank_test?charset=utf8',userName = 'dtstack',password = 'abc123',tableName = 't03_deposit_acct_base_info',cache = 'LRU',cacheSize = '10000',cacheTTLMs = '60000',parallelism = '1');CREATE TABLE bal_cur_batch_t04_org_cur_cur_input ( org_id VARCHAR, org_nm VARCHAR, org_short_nm VARCHAR, up_lvl_org_id VARCHAR, org_lvl VARCHAR, org_sort VARCHAR, org_cur VARCHAR,PRIMARY KEY (org_id) , PERIOD FOR SYSTEM_TIME ) WITH (type = 'mysql',url = 'jdbc:mysql://172.16.8.104:3306/bank_test?charset=utf8',userName = 'dtstack',password = 'abc123',tableName = 't04_org_cur',cache = 'LRU',cacheSize = '10000',cacheTTLMs = '60000',parallelism = '1');CREATE TABLE max_deposit_acct_base ( max_data_dt varchar, PRIMARY KEY (max_data_dt) , PERIOD FOR SYSTEM_TIME ) WITH (type = 'mysql',url = 'jdbc:mysql://172.16.8.104:3306/bank_test?charset=utf8',userName = 'dtstack',password = 'abc123',tableName = 'max_deposit_acct_base',cache = 'LRU',cacheSize = '10000',cacheTTLMs = '60000',parallelism = '1');\n" +
                "INSERT     \n" +
                "INTO  resultbank  select d.run_date,\n" +
                "        d.run_time,\n" +
                "        d.trf_flowno,\n" +
                "        d.trf_payname,\n" +
                "        d.trf_payacc,\n" +
                "        d.trf_tranamet,\n" +
                "        d.trf_rcvbank,\n" +
                "        d.org_nm \n" +
                "    from\n" +
                "    (\n" +
                "    select\n" +
                "        cast(current_timestamp as varchar) as run_date,\n" +
                "        SUBSTRING(cast(current_timestamp as varchar) from  1 for 16) as run_time,\n" +
                "        b.trf_flowno,\n" +
                "        b.trf_payname,\n" +
                "        b.trf_payacc,\n" +
                "        b.trf_tranamet,\n" +
                "        b.trf_rcvbank,\n" +
                "        b.data_dt,\n" +
                "        t3.org_nm \n" +
                "    from\n" +
                "        ( select\n" +
                "            a.trf_flowno,\n" +
                "            a.currTime,\n" +
                "            a.trf_payname,\n" +
                "            a.trf_tranamet,\n" +
                "            a.trf_rcvbank,\n" +
                "            a.trf_payacc,\n" +
                "            a.trf_subtime,\n" +
                "            a.trf_bsncode,\n" +
                "            t2.acct_org,\n" +
                "            t2.data_dt,\n" +
                "            current_timestamp as nowtime\n" +
                "        from\n" +
                "            (select\n" +
                "                t1.trf_flowno,\n" +
                "                t1.currTime,\n" +
                "                t1.trf_payname,\n" +
                "                t1.trf_tranamet,\n" +
                "                t1.trf_rcvbank,\n" +
                "                t1.trf_subtime,\n" +
                "                t1.trf_payacc,\n" +
                "                t1.trf_bsncode\n" +
                "            from\n" +
                "                bal_cur_batch_s30_pb_tranflow_input t1 \n" +
                "            join\n" +
                "                bal_cur_batch_rds_report_lrlct_trans_cur_input x \n" +
                "                    on t1.trf_flowno = x.trf_flowno \n" +
                "                    and x.run_date = t1.currTime ) as a \n" +
                "        join\n" +
                "            bal_cur_batch_t03_deposit_acct_base_info_cur_input t2 \n" +
                "                on a.trf_payacc = t2.card_id \n" +
                "            ) as b \n" +
                "    join\n" +
                "        bal_cur_batch_t04_org_cur_cur_input t3 \n" +
                "            on b.acct_org = t3.org_id\n" +
                "            where \n" +
                "    b.trf_bsncode in('002002', '002003', '002011')\n" +
                "    and b.trf_flowno is null \n" +
                "    and substring(b.trf_subtime from 1 for 8) = DATE_FORMAT(b.nowtime, '%Y%m%d')\n" +
                "    and cast(b.trf_tranamet as decimal) >= 100000000\n" +
                "    ) as d\n" +
                "  join max_deposit_acct_base maxdep\n" +
                "    on d.data_dt = maxdep.max_data_dt\n";
        test(sql);
    }

    @Test
    public void testRunHbaseSideTable() throws Exception {
        String sql = "CREATE TABLE MyTable ( name string, channel STRING, pv INT, xctime bigint, CHARACTER_LENGTH(channel) AS timeLeng) " +
                "WITH (type = 'kafka09',bootstrapServers = '172.16.8.198:9092',zookeeperQuorum = '172.16.8.198:2181/kafka', " +
                "offsetReset = 'latest',topic = 'nbTest1',parallelism = '1');" +
                "CREATE TABLE MyResult ( channel VARCHAR, pv VARCHAR) WITH (type = 'mysql'," +
                "url = 'jdbc:mysql://172.16.8.104:3306/test?charset=utf8',userName = 'dtstack',password = 'abc123',tableName = 'pv2'," +
                "parallelism = '1');CREATE TABLE workerinfo ( cast(logtime as TIMESTAMP) AS rtime, cast(logtime) AS rtime) " +
                "WITH (type = 'hbase',zookeeperQuorum = 'rdos1:2181',tableName = 'workerinfo',rowKey = 'ce,de'," +
                "parallelism = '1',zookeeperParent = '/hbase');CREATE TABLE sideTable " +
                "( cf:name String as name, cf:info String as info,PRIMARY KEY (name) , PERIOD FOR SYSTEM_TIME ) WITH " +
                "(type = 'hbase',zookeeperQuorum = 'rdos1:2181',zookeeperParent = '/hbase',tableName = 'workerinfo',cache = 'LRU'," +
                "cacheSize = '10000',cacheTTLMs = '60000',parallelism = '1');" +
                "insert  \n" +
                "into\n" +
                "    MyResult\n" +
                "    select d.channel,d.info\n" +
                "    from\n" +
                "    (\n" +
                "    select\n" +
                "        *\n" +
                "    from\n" +
                "        MyTable a  \n" +
                "    join\n" +
                "        sideTable b  \n" +
                "            on a.channel=b.name                       \n" +
                "    where\n" +
                "        a.channel = 'xc2' \n" +
                "        and a.pv=10\n" +
                "    ) as d\n";

        test(sql);
    }

    @Test
    public void testMysqlAllCache() throws Exception {
        String sql = "CREATE TABLE MyTable(\n" +
                "    channel STRING,\n" +
                "    pv INT,\n" +
                "    xctime bigint,\n" +
                "    CHARACTER_LENGTH(channel) as timeLeng,\n" +
                "    WATERMARK FOR xctime AS withOffset(xctime,1000)\n" +
                " )WITH(\n" +
                "    type='kafka09',\n" +
                "    bootstrapServers='172.16.8.198:9092',\n" +
                "    offsetReset='latest',\n" +
                "    topic='nbTest1'\n" +
                " );\n" +
                "CREATE TABLE MyResult(\n" +
                "    channel STRING,\n" +
                "    pv INT\n" +
                " )WITH(\n" +
                "    type='mysql',\n" +
                "    url='jdbc:mysql://172.16.8.104:3306/test?charset=utf8',\n" +
                "    userName='dtstack',\n" +
                "    password='abc123',\n" +
                "    tableName='pv'\n" +
                " );\n" +
                "create table sideTable(\n" +
                "    channel String,\n" +
                "    xccount int,\n" +
                "    PRIMARY KEY(channel),\n" +
                "    PERIOD FOR SYSTEM_TIME\n" +
                " )WITH(\n" +
                "    type='mysql',\n" +
                "    url='jdbc:mysql://172.16.8.104:3306/test?charset=utf8',\n" +
                "    userName='dtstack',\n" +
                "    password='abc123',\n" +
                "    tableName='sidetest',\n" +
                "    cache = 'ALL'\n" +
                " );\n" +
                "insert \n" +
                "into\n" +
                "    MyResult\n" +
                "    select\n" +
                "        a.channel,\n" +
                "        b.xccount \n" +
                "    from\n" +
                "        MyTable a \n" +
                "    join\n" +
                "        sideTable b \n" +
                "            on a.channel=b.channel \n" +
                "    where\n" +
                "        b.channel = 'xc' \n" +
                "        and a.pv=10";

        test(sql);
    }

    public void test(String sql) throws Exception {
        List<String> paramList = Lists.newArrayList();
        paramList.add("-sql");
        String exeSql = URLEncoder.encode(sql, Charsets.UTF_8.name());
        paramList.add(exeSql);
        paramList.add("-name");
        paramList.add("xc");
        paramList.add("-localSqlPluginPath");
        paramList.add("D:\\soucecode\\flinkStreamSQL\\plugins");
        paramList.add("-mode");
        paramList.add("local");
        paramList.add("-addjar");
        paramList.add(URLEncoder.encode("[\"D:\\\\soucecode\\\\rdos-execution-engine\\\\..\\\\tmp140\\\\flink14Test-1.0-SNAPSHOT.jar\"]", Charsets.UTF_8.name()));
        paramList.add("-remoteSqlPluginPath");
        paramList.add("/opt/dtstack/flinkplugin");
        paramList.add("-confProp");
        String conf = "{\"time.characteristic\":\"EventTime\",\"sql.checkpoint.interval\":10000}";
        String confEncode = URLEncoder.encode(conf, Charsets.UTF_8.name());
        paramList.add(confEncode);

        String[] params = new String[paramList.size()];
        paramList.toArray(params);
        Main.main(params);
    }

    @Test
    public void testParseSql() throws Exception {
        String sql = "CREATE TABLE bal_cur_batch_s30_pb_tranflow_input (trf_flowno varchar, trf_batno varchar, trf_cstno varchar, trf_bsncode varchar, trf_transtype varchar, trf_payacc varchar, trf_paysubacc varchar, trf_payname varchar, trf_rcvacc varchar, trf_rcvsubacc varchar, trf_rcvname varchar, trf_rcvbank varchar, trf_comitrno varchar, trf_crytype varchar, trf_subtime varchar, trf_tranamet varchar, trf_fee1 varchar, trf_fee2 varchar, trf_fee3 varchar, trf_userrem varchar, trf_hostflwno varchar, trf_hostsendtime varchar, trf_hosterror varchar, trf_lastupdtime varchar, trf_stt varchar, trf_schl_flowno varchar, trf_logontype varchar, trf_reccity varchar, trf_recprovice varchar, trf_channel varchar, trf_hosterrormsg varchar, trf_ext1 varchar, trf_ext2 varchar, trf_security varchar, cast(current_timestamp as varchar) as currTime) WITH (type = 'kafka09',bootstrapServers = 'kudu1:9092',zookeeperQuorum = '172.16.8.107:2181/kafka', offsetReset = 'latest',topic = 'tranflow_input',parallelism = '1');CREATE TABLE resultbank ( run_date VARCHAR, run_time VARCHAR, trf_flowno VARCHAR, trf_payname VARCHAR, trf_payacc VARCHAR, trf_tranamet VARCHAR, trf_subtime VARCHAR, trf_rcvbank VARCHAR,PRIMARY KEY (run_date,run_time,trf_flowno) ) WITH (type = 'mysql',url = 'jdbc:mysql://172.16.8.104:3306/bank_test?charset=utf8',userName = 'dtstack',password = 'abc123',tableName = 'resultbank',parallelism = '1');CREATE TABLE bal_cur_batch_rds_report_lrlct_trans_cur_input ( run_date VARCHAR, run_time VARCHAR, trf_flowno VARCHAR, trf_payname VARCHAR, trf_payacc VARCHAR, trf_subtime VARCHAR, trf_rcvbank VARCHAR,PRIMARY KEY (run_date,trf_flowno) , PERIOD FOR SYSTEM_TIME ) WITH (type = 'mysql',url = 'jdbc:mysql://172.16.8.104:3306/bank_test?charset=utf8',userName = 'dtstack',password = 'abc123',tableName = 'rds_report_lrlc_trans_cur',cache = 'LRU',cacheSize = '10000',cacheTTLMs = '60000',parallelism = '1');CREATE TABLE bal_cur_batch_t03_deposit_acct_base_info_cur_input ( data_dt VARCHAR, card_id VARCHAR, sub_acct_id VARCHAR, acct_org varchar, acct_stat_cd VARCHAR, core_cust_id VARCHAR, cust_rm VARCHAR, cust_scale_cd VARCHAR, item_id VARCHAR,PRIMARY KEY (card_id) , PERIOD FOR SYSTEM_TIME ) WITH (type = 'mysql',url = 'jdbc:mysql://172.16.8.104:3306/bank_test?charset=utf8',userName = 'dtstack',password = 'abc123',tableName = 't03_deposit_acct_base_info',cache = 'LRU',cacheSize = '10000',cacheTTLMs = '60000',parallelism = '1');CREATE TABLE bal_cur_batch_t04_org_cur_cur_input ( org_id VARCHAR, org_nm VARCHAR, org_short_nm VARCHAR, up_lvl_org_id VARCHAR, org_lvl VARCHAR, org_sort VARCHAR, org_cur VARCHAR,PRIMARY KEY (org_id) , PERIOD FOR SYSTEM_TIME ) WITH (type = 'mysql',url = 'jdbc:mysql://172.16.8.104:3306/bank_test?charset=utf8',userName = 'dtstack',password = 'abc123',tableName = 't04_org_cur',cache = 'LRU',cacheSize = '10000',cacheTTLMs = '60000',parallelism = '1');CREATE TABLE max_deposit_acct_base ( max_data_dt varchar, PRIMARY KEY (max_data_dt) , PERIOD FOR SYSTEM_TIME ) WITH (type = 'mysql',url = 'jdbc:mysql://172.16.8.104:3306/bank_test?charset=utf8',userName = 'dtstack',password = 'abc123',tableName = 'max_deposit_acct_base',cache = 'LRU',cacheSize = '10000',cacheTTLMs = '60000',parallelism = '1');\n" +
                "INSERT     \n" +
                "INTO  resultbank  select d.run_date,\n" +
                "        d.run_time,\n" +
                "        d.trf_flowno,\n" +
                "        d.trf_payname,\n" +
                "        d.trf_payacc,\n" +
                "        d.trf_tranamet,\n" +
                "        d.trf_rcvbank,\n" +
                "        d.org_nm \n" +
                "    from\n" +
                "    (\n" +
                "    select\n" +
                "        cast(current_timestamp as varchar) as run_date,\n" +
                "        SUBSTRING(cast(current_timestamp as varchar) from  1 for 16) as run_time,\n" +
                "        b.trf_flowno,\n" +
                "        b.trf_payname,\n" +
                "        b.trf_payacc,\n" +
                "        b.trf_tranamet,\n" +
                "        b.trf_rcvbank,\n" +
                "        b.data_dt,\n" +
                "        t3.org_nm \n" +
                "    from\n" +
                "        ( select\n" +
                "            a.trf_flowno,\n" +
                "            a.currTime,\n" +
                "            a.trf_payname,\n" +
                "            a.trf_tranamet,\n" +
                "            a.trf_rcvbank,\n" +
                "            a.trf_payacc,\n" +
                "            a.trf_subtime,\n" +
                "            a.trf_bsncode,\n" +
                "            t2.acct_org,\n" +
                "            t2.data_dt,\n" +
                "            current_timestamp as nowtime\n" +
                "        from\n" +
                "            (select\n" +
                "                t1.trf_flowno,\n" +
                "                t1.currTime,\n" +
                "                t1.trf_payname,\n" +
                "                t1.trf_tranamet,\n" +
                "                t1.trf_rcvbank,\n" +
                "                t1.trf_subtime,\n" +
                "                t1.trf_payacc,\n" +
                "                t1.trf_bsncode\n" +
                "            from\n" +
                "                bal_cur_batch_s30_pb_tranflow_input t1 \n" +
                "            join\n" +
                "                bal_cur_batch_rds_report_lrlct_trans_cur_input x \n" +
                "                    on t1.trf_flowno = x.trf_flowno \n" +
                "                    and x.run_date = t1.currTime ) as a \n" +
                "        join\n" +
                "            bal_cur_batch_t03_deposit_acct_base_info_cur_input t2 \n" +
                "                on a.trf_payacc = t2.card_id \n" +
                "            ) as b \n" +
                "    join\n" +
                "        bal_cur_batch_t04_org_cur_cur_input t3 \n" +
                "            on b.acct_org = t3.org_id\n" +
                "            where \n" +
                "    b.trf_bsncode in('002002', '002003', '002011')\n" +
                "    and b.trf_flowno is null \n" +
                "    and substring(b.trf_subtime from 1 for 8) = DATE_FORMAT(b.nowtime, '%Y%m%d')\n" +
                "    and cast(b.trf_tranamet as decimal) >= 100000000\n" +
                "    ) as d\n" +
                "  join max_deposit_acct_base maxdep\n" +
                "    on d.data_dt = maxdep.max_data_dt\n";
//        sql = URLDecoder.decode(sql, org.apache.commons.io.Charsets.UTF_8.name());
        String localSqlPluginPath = "D:\\soucecode\\flinkStreamSQL\\plugins";
        SqlParser.setLocalSqlPluginRoot(localSqlPluginPath);
        SqlTree sqlTree = SqlParser.parseSql(sql);
        System.out.println("1111");
    }

    @Test
    public void testParseSql2() throws Exception {
        String sql = "CREATE TABLE MyTable(\n" +
                "    name varchar,\n" +
                "    channel varchar\n" +
                " )WITH(\n" +
                "    type ='kafka10',\n" +
                "    bootstrapServers ='172.21.32.1:9092',\n" +
                "    zookeeperQuorum ='172.21.32.1:2181/kafka',\n" +
                "    offsetReset ='earliest',\n" +
                "    topic ='test1',\n" +
                "    parallelism ='3'\n" +
                " );\n" +
                " \n" +
                " CREATE TABLE MyResult(\n" +
                "    name varchar,\n" +
                "    channel varchar\n" +
                " )WITH(\n" +
                "    type ='mysql',\n" +
                "    url ='jdbc:mysql://127.0.0.1:3306/test?charset=utf8&useSSL=false',\n" +
                "    userName ='root',\n" +
                "    password ='123456',\n" +
                "    tableName ='pv',\n" +
                "    parallelism ='3'\n" +
                " );\n" +
                " \n" +
                "insert into MyResult\n" +
                "select a.name,a.channel from MyTable a";
        test2(sql);
    }

    @Test
    public void testParseMongo() throws Exception {
        String sql = "CREATE TABLE MyTable(\n" +
                "    name varchar,\n" +
                "    channel varchar\n" +
                " )WITH(\n" +
                "    type ='kafka10',\n" +
                "    bootstrapServers ='172.21.32.1:9092',\n" +
                "    zookeeperQuorum ='172.21.32.1:2181/kafka',\n" +
                "    offsetReset ='earliest',\n" +
                "    topic ='test',\n" +
                "    parallelism ='3'\n" +
                " );\n" +
                " \n" +
                " CREATE TABLE MyResult(\n" +
                "    name varchar,\n" +
                "    channel varchar,\n" +
                "\txccount int\n" +
                " )WITH(\n" +
                "    type ='mongo',\n" +
                "    address ='172.21.32.1:27017,172.21.32.1:27017',\n" +
                "    database ='test',\n" +
                "    tableName ='pv',\n" +
                "    parallelism ='3'\n" +
                " );\n" +
                " \n" +
                "create table sideTable(\n" +
                "    channel varchar,\n" +
                "    xccount int,\n" +
                "    PRIMARY KEY(channel),\n" +
                "    PERIOD FOR SYSTEM_TIME\n" +
                " )WITH(\n" +
                "    type='mysql',\n" +
                "    url='jdbc:mysql://127.0.0.1:3306/test?charset=utf8&useSSL=true',\n" +
                "    userName='root',\n" +
                "    password='123456',\n" +
                "    tableName='sidetest',\n" +
                "    cache ='NONE',\n" +
                "    cacheSize ='10000',\n" +
                "    cacheTTLMs ='60000',\n" +
                "    parallelism ='1',\n" +
                "    partitionedJoin='false'\n" +
                " );\n" +
                " \n" +
                "\n" +
                "insert into MyResult\n" +
                "select a.name,a.channel,b.xccount\n" +
                "from MyTable a join sideTable b\n" +
                "on a.channel=b.channel;\n";
        test2(sql);
    }

    @Test
    public void testParseMongo2() throws Exception {
        String sql = "CREATE TABLE MyTable(\n" +
                "    name varchar,\n" +
                "    channel varchar\n" +
                " )WITH(\n" +
                "    type ='kafka10',\n" +
                "    bootstrapServers ='172.21.32.1:9092',\n" +
                "    zookeeperQuorum ='172.21.32.1:2181/kafka',\n" +
                "    offsetReset ='earliest',\n" +
                "    topic ='test1',\n" +
                "    parallelism ='3'\n" +
                " );\n" +
                " \n" +
                " CREATE TABLE MyResult(\n" +
                "    name varchar,\n" +
                "    channel varchar,\n" +
                "\txccount int\n" +
                " )WITH(\n" +
                "    type ='mongo',\n" +
                "    address ='172.21.32.1:27017,172.21.32.1:27017',\n" +
                "    database ='test',\n" +
                "    tableName ='pv',\n" +
                "    parallelism ='3'\n" +
                " );\n" +
                " \n" +
                "create table sideTable(\n" +
                "    CHANNEL varchar,\n" +
                "    XCCOUNT int,\n" +
                "    PRIMARY KEY(channel),\n" +
                "    PERIOD FOR SYSTEM_TIME\n" +
                " )WITH(\n" +
                "    type ='mongo',\n" +
                "    address ='172.21.32.1:27017,172.21.32.1:27017',\n" +
                "    database ='test',\n" +
                "    tableName ='sidetest',\n" +
                "    cache ='ALL',\n" +
                "    parallelism ='1',\n" +
                "    partitionedJoin='false'\n" +
                " );\n" +
                " \n" +
                "insert into MyResult\n" +
                "select a.name,a.channel,b.xccount\n" +
                "from MyTable a join sideTable b\n" +
                "on a.channel=b.channel;\n";
        test2(sql);
    }

    public void test2(String sql) throws Exception {
        List<String> paramList = Lists.newArrayList();
        paramList.add("-sql");
        String exeSql = URLEncoder.encode(sql, Charsets.UTF_8.name());
        paramList.add(exeSql);
        paramList.add("-name");
        paramList.add("xc");
        paramList.add("-localSqlPluginPath");
        paramList.add("D:\\soucecode\\flinkStreamSQL-my-src\\plugins");
        paramList.add("-mode");
        paramList.add("local");
        paramList.add("-confProp");
        String conf = "{\"time.characteristic\":\"ProcessingTime\",\"sql.checkpoint.interval\":10000}";
        String confEncode = URLEncoder.encode(conf, Charsets.UTF_8.name());
        paramList.add(confEncode);

        String[] params = new String[paramList.size()];
        paramList.toArray(params);
        Main.main(params);
    }
}
