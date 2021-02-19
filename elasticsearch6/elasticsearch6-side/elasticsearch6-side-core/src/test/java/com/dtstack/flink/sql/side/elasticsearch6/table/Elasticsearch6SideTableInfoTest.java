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

package com.dtstack.flink.sql.side.elasticsearch6.table;

import org.junit.Test;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Company: www.dtstack.com
 *
 * @author zhufeng
 * @date 2020-07-02
 */
public class Elasticsearch6SideTableInfoTest {
    Elasticsearch6SideTableInfo tableInfo = new Elasticsearch6SideTableInfo();

    @Test
    public void getAndSetTest() throws InvocationTargetException, IntrospectionException,
            InstantiationException, IllegalAccessException {
        this.testGetAndSet();
    }

    @Test
    public void getTypeTest() {
        tableInfo.getType();
    }

    private void testGetAndSet() throws IllegalAccessException, InstantiationException, IntrospectionException,
            InvocationTargetException {
        Elasticsearch6SideTableInfo t = new Elasticsearch6SideTableInfo();
        Class modelClass = t.getClass();
        Object obj = modelClass.newInstance();
        Field[] fields = modelClass.getDeclaredFields();
        for (Field f : fields) {
            if (!f.getName().equals("CURR_TYPE")) {
                if (f.getName().equals("aLike")
                        || f.isSynthetic()) {
                    continue;
                }

                PropertyDescriptor pd = new PropertyDescriptor(f.getName(), modelClass);
                Method get = pd.getReadMethod();
                Method set = pd.getWriteMethod();
                set.invoke(obj, get.invoke(obj));
            }
        }
    }
}
