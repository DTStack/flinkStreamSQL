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

package com.dtstack.flink.sql.side.redis.table;

import com.dtstack.flink.sql.side.ISideReqRow;
import com.dtstack.flink.sql.side.BaseSideInfo;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.types.Row;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Map;

/**
 * redis fill row data
 * Date: 2018/12/4
 * Company: www.dtstack.com
 * @author xuchao
 */

public class RedisSideReqRow implements ISideReqRow, Serializable {

    private static final long serialVersionUID = 3751171828444748982L;

    private BaseSideInfo sideInfo;

    public RedisSideReqRow(BaseSideInfo sideInfo){
        this.sideInfo = sideInfo;
    }

    @Override
    public Row fillData(Row input, Object sideInput) {
        Map<String, String> sideInputMap = (Map<String, String>) sideInput;
        Row row = new Row(sideInfo.getOutFieldInfoList().size());
        for(Map.Entry<Integer, Integer> entry : sideInfo.getInFieldIndex().entrySet()){
            Object obj = input.getField(entry.getValue());
            boolean isTimeIndicatorTypeInfo = TimeIndicatorTypeInfo.class.isAssignableFrom(sideInfo.getRowTypeInfo().getTypeAt(entry.getValue()).getClass());

            if(obj instanceof Timestamp && isTimeIndicatorTypeInfo){
                obj = ((Timestamp)obj).getTime();
            }
            row.setField(entry.getKey(), obj);
        }

        for(Map.Entry<Integer, Integer> entry : sideInfo.getSideFieldIndex().entrySet()){
            if(sideInputMap == null){
                row.setField(entry.getKey(), null);
            }else{
                String key = sideInfo.getSideFieldNameIndex().get(entry.getKey());
                setRowField(row, entry.getKey(), sideInfo, sideInputMap.get(key));
            }
        }

        return row;
    }

    public void setRowField(Row row, Integer index, BaseSideInfo sideInfo, String value) {
        Integer keyIndex = sideInfo.getSideFieldIndex().get(index);
        String classType = sideInfo.getSideTableInfo().getFieldClassList().get(keyIndex).getName();
        switch (classType){
            case "java.lang.Integer":
                row.setField(index, Integer.valueOf(value));
                break;
            case "java.lang.String":
                row.setField(index, value);
                break;
            case "java.lang.Double":
                row.setField(index, Double.valueOf(value));
                break;
            case "java.lang.Long":
                row.setField(index, Long.valueOf(value));
                break;
            case "java.lang.Byte":
                row.setField(index, Byte.valueOf(value));
                break;
            case "java.lang.Short":
                row.setField(index, Short.valueOf(value));
                break;
            case "java.lang.Float":
                row.setField(index, Float.valueOf(value));
                break;
            case "java.math.BigDecimal":
                row.setField(index, BigDecimal.valueOf(Long.valueOf(value)));
                break;
            case "java.sql.Timestamp":
                row.setField(index, Timestamp.valueOf(value));
                break;
            case "java.sql.Date":
                row.setField(index, Date.valueOf(value));
                break;
            default:
                throw new RuntimeException("no support field type. the type: " + classType);
        }
    }

}
