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

import org.apache.calcite.sql.parser.SqlParseException;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Date: 2019/12/18
 * Company: www.dtstack.com
 * @author maqi
 */
public class SidePredicatesParserTest {

    @Test
    public void testfillPredicatesForSideTable() throws SqlParseException {
        String testSql =
                "insert  \n" +
                        "into\n" +
                        "    MyResult\n" +
                        "    select\n" +
                        "        MyTable.a,\n" +
                        "        MyTable.b,\n" +
                        "        s.c,\n" +
                        "        s.d\n" +
                        "    from\n" +
                        "        MyTable\n" +
                        "    join\n" +
                        "        sideTable s\n" +
                        "            on   MyTable.a = s.c\n" +
                        "    where\n" +
                        "           MyTable.a='1' and s.d='1' and  s.d <> '3' and s.c LIKE '%xx%' and s.c in ('1','2') and s.c between '10' and '23' and s.d is not null\n";


        SideTableInfo sideTableInfo = new SideTableInfo(){
            @Override
            public boolean check() {
                return false;
            }
        };

        sideTableInfo.setName("sideTable");

        Map<String, SideTableInfo> sideTableMap = new HashMap<>();
        sideTableMap.put("sideTable", sideTableInfo);

        SidePredicatesParser sidePredicatesParser = new SidePredicatesParser();
        sidePredicatesParser.fillPredicatesForSideTable(testSql, sideTableMap);
        List<PredicateInfo> sideTablePredicateInfoes = sideTableMap.get("sideTable").getPredicateInfoes();

        List<PredicateInfo> expectedsPredicateInfoes = new ArrayList<>();
        expectedsPredicateInfoes.add(PredicateInfo.builder().setOperatorName("=").setOperatorKind("EQUALS").setOwnerTable("s").setFieldName("d").setCondition("'1'").build());
        expectedsPredicateInfoes.add(PredicateInfo.builder().setOperatorName("<>").setOperatorKind("NOT_EQUALS").setOwnerTable("s").setFieldName("d").setCondition("'3'").build());
        expectedsPredicateInfoes.add(PredicateInfo.builder().setOperatorName("LIKE").setOperatorKind("LIKE").setOwnerTable("s").setFieldName("c").setCondition("'%xx%'").build());
        expectedsPredicateInfoes.add(PredicateInfo.builder().setOperatorName("IN").setOperatorKind("IN").setOwnerTable("s").setFieldName("c").setCondition("'1', '2'").build());
        expectedsPredicateInfoes.add(PredicateInfo.builder().setOperatorName("BETWEEN ASYMMETRIC").setOperatorKind("BETWEEN").setOwnerTable("s").setFieldName("c").setCondition("'10' AND '23'").build());
        expectedsPredicateInfoes.add(PredicateInfo.builder().setOperatorName("IS NOT NULL").setOperatorKind("IS_NOT_NULL").setOwnerTable("s").setFieldName("d").setCondition("s.d").build());


        Assert.assertEquals(expectedsPredicateInfoes.toString(), sideTablePredicateInfoes.toString());
    }
}
