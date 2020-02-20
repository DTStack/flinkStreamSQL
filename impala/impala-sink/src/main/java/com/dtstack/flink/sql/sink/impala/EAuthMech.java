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

package com.dtstack.flink.sql.sink.impala;

/**
 *  impala kdnc AuthMech params
 * Date: 2020/2/18
 * Company: www.dtstack.com
 * @author maqi
 */
public enum EAuthMech {
    // 0 for No Authentication
    NoAuthentication(0),
    // 1 for Kerberos
    Kerberos(1),
    // 2 for User Name
    UserName(2),
    // 3 for User Name and Password
    NameANDPassword(3);

    private int type;

    EAuthMech(int type) {
        this.type = type;
    }

    public int getType() {
        return this.type;
    }
}
