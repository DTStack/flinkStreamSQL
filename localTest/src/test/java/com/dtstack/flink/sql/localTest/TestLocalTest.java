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

package com.dtstack.flink.sql.localTest;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * @author tiezhu
 * @Date 2020/7/8 Wed
 * Company dtstack
 */
public class TestLocalTest {

    @Test
    public void testReadSQL() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String result = "this is a test";
        String sqlPath = "/Users/wtz4680/Desktop/ideaProject/flinkStreamSQL/localTest/src/test/resources/test.txt";
        Class<LocalTest> testClass = LocalTest.class;
        Method method = testClass.getDeclaredMethod("readSQL", String.class);
        method.setAccessible(true);
        Assert.assertEquals(result, method.invoke(new LocalTest(), sqlPath));
    }
}
