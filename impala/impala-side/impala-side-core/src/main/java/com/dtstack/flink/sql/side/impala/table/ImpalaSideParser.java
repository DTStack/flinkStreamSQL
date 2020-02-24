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

package com.dtstack.flink.sql.side.impala.table;

import com.dtstack.flink.sql.side.rdb.table.RdbSideParser;
import com.dtstack.flink.sql.table.TableInfo;
import com.dtstack.flink.sql.util.MathUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Reason:
 * Date: 2019/11/12
 * Company: www.dtstack.com
 *
 * @author xiuzhu
 */

public class ImpalaSideParser extends RdbSideParser {
    private static final Logger LOG = LoggerFactory.getLogger(ImpalaSideParser.class);

    private static final String CURR_TYPE = "impala";

    @Override
    public TableInfo getTableInfo(String tableName, String fieldsInfo, Map<String, Object> props) {
        ImpalaSideTableInfo impalaSideTableInfo = new ImpalaSideTableInfo();
        impalaSideTableInfo.setType(CURR_TYPE);
        impalaSideTableInfo.setName(tableName);
        parseFieldsInfo(fieldsInfo, impalaSideTableInfo);

        parseCacheProp(impalaSideTableInfo, props);
        impalaSideTableInfo.setParallelism(MathUtil.getIntegerVal(props.get(ImpalaSideTableInfo.PARALLELISM_KEY.toLowerCase())));
        impalaSideTableInfo.setUrl(MathUtil.getString(props.get(ImpalaSideTableInfo.URL_KEY.toLowerCase())));
        impalaSideTableInfo.setTableName(MathUtil.getString(props.get(ImpalaSideTableInfo.TABLE_NAME_KEY.toLowerCase())));
        impalaSideTableInfo.setUserName(MathUtil.getString(props.get(ImpalaSideTableInfo.USER_NAME_KEY.toLowerCase())));
        impalaSideTableInfo.setPassword(MathUtil.getString(props.get(ImpalaSideTableInfo.PASSWORD_KEY.toLowerCase())));
        impalaSideTableInfo.setSchema(MathUtil.getString(props.get(ImpalaSideTableInfo.SCHEMA_KEY.toLowerCase())));


        //set authmech params
        Integer authMech = MathUtil.getIntegerVal(props.get(ImpalaSideTableInfo.AUTHMECH_KEY.toLowerCase()));

        authMech = authMech == null? 0 : authMech;
        impalaSideTableInfo.setAuthMech(authMech);
        List authMechs = Arrays.asList(new Integer[]{0, 1, 2, 3});

        if (!authMechs.contains(authMech)){
            throw new IllegalArgumentException("The value of authMech is illegal, Please select 0, 1, 2, 3");
        } else if (authMech == 1) {
            impalaSideTableInfo.setPrincipal(MathUtil.getString(props.get(ImpalaSideTableInfo.PRINCIPAL_KEY.toLowerCase())));
            impalaSideTableInfo.setKeyTabFilePath(MathUtil.getString(props.get(ImpalaSideTableInfo.KEYTABFILEPATH_KEY.toLowerCase())));
            impalaSideTableInfo.setKrb5FilePath(MathUtil.getString(props.get(ImpalaSideTableInfo.KRB5FILEPATH_KEY.toLowerCase())));
            String krbRealm = MathUtil.getString(props.get(ImpalaSideTableInfo.KRBREALM_KEY.toLowerCase()));
            krbRealm = krbRealm == null? "HADOOP.COM" : krbRealm;
            impalaSideTableInfo.setKrbRealm(krbRealm);
            impalaSideTableInfo.setKrbHostFQDN(MathUtil.getString(props.get(ImpalaSideTableInfo.KRBHOSTFQDN_KEY.toLowerCase())));
            impalaSideTableInfo.setKrbServiceName(MathUtil.getString(props.get(ImpalaSideTableInfo.KRBSERVICENAME_KEY.toLowerCase())));
        } else if (authMech == 2 ) {
            impalaSideTableInfo.setUserName(MathUtil.getString(props.get(ImpalaSideTableInfo.USER_NAME_KEY.toLowerCase())));
        } else if (authMech == 3) {
            impalaSideTableInfo.setUserName(MathUtil.getString(props.get(ImpalaSideTableInfo.USER_NAME_KEY.toLowerCase())));
            impalaSideTableInfo.setPassword(MathUtil.getString(props.get(ImpalaSideTableInfo.PASSWORD_KEY.toLowerCase())));
        }

        //set partition params
        String enablePartitionStr  = (String) props.get(ImpalaSideTableInfo.ENABLEPARTITION_KEY.toLowerCase());
        boolean enablePartition = MathUtil.getBoolean(enablePartitionStr == null? "false":enablePartitionStr);
        impalaSideTableInfo.setEnablePartition(enablePartition);
        if (enablePartition) {
            String partitionfieldsStr = MathUtil.getString(props.get(ImpalaSideTableInfo.PARTITIONFIELDS_KEY.toLowerCase()));
            impalaSideTableInfo.setPartitionfields(StringUtils.split(partitionfieldsStr, ","));
            String partitionfieldTypesStr = MathUtil.getString(props.get(ImpalaSideTableInfo.PARTITIONFIELDTYPES_KEY.toLowerCase()));
            impalaSideTableInfo.setPartitionFieldTypes(StringUtils.split(partitionfieldsStr, ","));
            String partitionfieldValuesStr = MathUtil.getString(props.get(ImpalaSideTableInfo.PARTITIONVALUES_KEY.toLowerCase()));
            impalaSideTableInfo.setPartitionValues(setPartitionFieldValues(partitionfieldValuesStr));
        }

        impalaSideTableInfo.check();
        return impalaSideTableInfo;
    }

    public Map setPartitionFieldValues(String partitionfieldValuesStr){
        Map<String, Object> fieldValues = new HashMap();
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            fieldValues = objectMapper.readValue(partitionfieldValuesStr, Map.class);
            for (String key : fieldValues.keySet()) {
                List value = (List)fieldValues.get(key);
                fieldValues.put(key, value);
            }
            return fieldValues;
        } catch (Exception e) {
            LOG.error("",e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public  Class dbTypeConvertToJavaType(String fieldType) {
        switch (fieldType.toLowerCase()) {
            case "boolean":
                return Boolean.class;
            case "char":
                return Character.class;
            case "double":
                return Double.class;
            case "float":
                return Float.class;
            case "tinyint":
                return Byte.class;
            case "smallint":
                return Short.class;
            case "int":
                return Integer.class;
            case "bigint":
                return Long.class;
            case "decimal":
                return BigDecimal.class;
            case "string":
            case "varchar":
                return String.class;
            case "timestamp":
                return Timestamp.class;
        }

        throw new RuntimeException("不支持 " + fieldType + " 类型");
    }
}
