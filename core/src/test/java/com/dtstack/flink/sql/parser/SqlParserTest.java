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

package com.dtstack.flink.sql.parser;

import org.junit.Assert;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.util.ArrayList;
import java.util.List;

public class SqlParserTest {

    @Test
    public void testRemoveAddFileStmt() throws Exception {
        List<String> rawStmts = new ArrayList<>();
        String sql1 = "        add    file     asdasdasd   ";
        String sql2 = "        aDd    fIle    With    asdasdasd   ";
        String sql3 = "        INSERT INTO dwd_foo SELECT id, name FROM ods_foo";
        String sql4 = "        ADD  FILE   asb    ";
        rawStmts.add(sql1);
        rawStmts.add(sql2);
        rawStmts.add(sql3);
        rawStmts.add(sql4);

        List<String> stmts = Whitebox.invokeMethod(SqlParser.class, "removeAddFileStmt", rawStmts);
        Assert.assertEquals(stmts.get(0), sql3);
    }

}