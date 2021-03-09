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

package com.dtstack.flink.sql.source.file.table;

import com.dtstack.flink.sql.table.AbstractSourceParser;
import com.dtstack.flink.sql.table.AbstractTableInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.csv.CsvRowDeserializationSchema;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author tiezhu
 * @date 2021/3/10 星期三
 * Company dtstack
 */
public class FileSourceTableInfoTest {
    private static final AbstractSourceParser parser = new FileSourceParser();
    Map<String, Object> props;

    @Before
    public void setUp() {
        props = new HashMap<>();
        props.put("format", "csv");
        props.put("type", "file");
        props.put("filename", "testFile");
        props.put("filepath", "./");
    }

    @Test
    public void testGetTableInfoWithCsv() throws Exception {
        AbstractTableInfo tableInfo = parser.getTableInfo(
            "Test",
            "id int, name string",
            props
        );
        FileSourceTableInfo fileSourceTableInfo = (FileSourceTableInfo) tableInfo;
        Assert.assertEquals(fileSourceTableInfo.getLocation(), "local");
        TypeInformation<Row> information = fileSourceTableInfo.getRowTypeInformation();
        System.out.println(information);
        Assert.assertEquals(fileSourceTableInfo.getOperatorName(), "testFile_Test");
        Assert.assertEquals(
            CsvRowDeserializationSchema.class,
            fileSourceTableInfo.buildDeserializationSchema().getClass()
        );
    }

}
