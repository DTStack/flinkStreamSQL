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

package com.dtstack.flink.sql.sink.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;
import static org.powermock.api.support.membermodification.MemberModifier.suppress;

/**
 * @author: chuixue
 * @create: 2020-08-12 15:35
 * @description:
 **/
@RunWith(PowerMockRunner.class)
@PrepareForTest({
        HbaseConfigUtils.class,
        UserGroupInformation.class})
public class HbaseConfigUtilsTest {

    @Test
    public void testLoginAndReturnUGI() throws Exception {
        suppress(UserGroupInformation.class.getMethod("setConfiguration", Configuration.class));
        suppress(UserGroupInformation.class.getMethod("loginUserFromKeytabAndReturnUGI", String.class, String.class));

        File myFile = mock(File.class);
        whenNew(File.class).withAnyArguments().thenReturn(myFile);
        when(myFile.exists()).thenReturn(true);

        HbaseConfigUtils.loginAndReturnUGI(new Configuration(), "principal", "keytab");
    }
}
