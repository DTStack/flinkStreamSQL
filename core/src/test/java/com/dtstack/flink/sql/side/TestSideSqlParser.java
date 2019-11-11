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
import com.google.common.collect.Sets;
import org.junit.Test;

import java.util.Set;

/**
 * Reason:
 * Date: 2018/7/24
 * Company: www.dtstack.com
 * @author xuchao
 */

public class TestSideSqlParser {

    @Test
    public void testSideSqlParser() throws SqlParseException {
        String sql = "select j1.id,j2.name,j1.info \n" +
                "   from\n" +
                "   (\n" +
                "   \tselect a.id,a.name,b.id \n" +
                "   \t\tfrom tab1 a join tab2 b\n" +
                "   \t\ton a.id = b.id  and a.proctime between b.proctime - interval '4' second and b.proctime + interval '4' second  \n" +
                "   ) j1\n" +
                "   join tab3 j2\n" +
                "   on j1.id = j2.id \n" +
                "   where j1.info like 'xc2'";

        Set<String> sideTbList = Sets.newHashSet("TAB3", "TAB4");


        SideSQLParser sideSQLParser = new SideSQLParser();
        sideSQLParser.getExeQueue(sql, sideTbList);
    }


}
