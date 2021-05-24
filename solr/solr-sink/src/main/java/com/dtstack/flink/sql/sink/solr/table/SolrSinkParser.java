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

package com.dtstack.flink.sql.sink.solr.table;

import com.dtstack.flink.sql.constant.PluginParamConsts;
import com.dtstack.flink.sql.sink.solr.options.KerberosOptions;
import com.dtstack.flink.sql.sink.solr.options.SolrClientOptions;
import com.dtstack.flink.sql.sink.solr.options.SolrWriteOptions;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import com.dtstack.flink.sql.table.AbstractTableParser;
import com.dtstack.flink.sql.util.MathUtil;
import org.apache.commons.lang3.StringUtils;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.dtstack.flink.sql.table.AbstractTableInfo.PARALLELISM_KEY;

/**
 * @author wuren
 * @program flinkStreamSQL
 * @create 2021/05/17
 */
public class SolrSinkParser extends AbstractTableParser {

    public static final String ZK_HOSTS = "zk-hosts";
    public static final String ZK_CHROOT = "zk-chroot";
    public static final String COLLECTION = "collection";

    public static final String SINK_BUFFER_FLUSH_MAX_ROWS = "sink.buffer-flush.max-rows";
    public static final String SINK_BUFFER_FLUSH_INTERVAL = "sink.buffer-flush.interval";

    @Override
    public AbstractTableInfo getTableInfo(
            String tableName, String fieldsInfo, Map<String, Object> props) throws Exception {
        SolrTableInfo solrTableInfo = new SolrTableInfo();
        solrTableInfo.setName(tableName);
        parseFieldsInfo(fieldsInfo, solrTableInfo);

        String zkHostsStr = MathUtil.getString(props.get(ZK_HOSTS));
        List<String> zkHosts = Arrays.asList(StringUtils.split(zkHostsStr, ','));
        SolrClientOptions solrClientOptions =
                new SolrClientOptions(
                        zkHosts,
                        MathUtil.getString(props.get(ZK_CHROOT)),
                        MathUtil.getString(props.get(COLLECTION)));
        solrTableInfo.setSolrClientOptions(solrClientOptions);

        SolrWriteOptions solrWriteOptions =
                new SolrWriteOptions(
                        Optional.ofNullable(
                                MathUtil.getLongVal(props.get(SINK_BUFFER_FLUSH_INTERVAL))),
                        Optional.ofNullable(
                                MathUtil.getIntegerVal(props.get(SINK_BUFFER_FLUSH_MAX_ROWS))),
                        Optional.ofNullable(
                                MathUtil.getIntegerVal(props.get(PARALLELISM_KEY.toLowerCase()))));
        solrTableInfo.setSolrWriteOptions(solrWriteOptions);

        KerberosOptions kerberosOptions =
                new KerberosOptions(
                        MathUtil.getString(props.get(PluginParamConsts.PRINCIPAL)),
                        MathUtil.getString(props.get(PluginParamConsts.KEYTAB)),
                        MathUtil.getString(props.get(PluginParamConsts.KRB5_CONF)));
        kerberosOptions.judgeKrbEnable();
        solrTableInfo.setKerberosOptions(kerberosOptions);

        return solrTableInfo;
    }

    @Override
    public Class dbTypeConvertToJavaType(String fieldType) {
        switch (fieldType.toLowerCase()) {
            case "bool":
            case "boolean":
                return Boolean.class;
            case "int":
            case "integer":
                return Integer.class;
            case "long":
            case "bigint":
                return Long.class;
            case "varchar":
            case "string":
            case "text":
                return String.class;
            case "float":
                return Float.class;
            case "double":
                return Double.class;
            case "date":
            case "timestamp":
                return Timestamp.class;
            default:
                throw new RuntimeException("不支持 " + fieldType + " 类型");
        }
    }
}
