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

import com.dtstack.flink.sql.enums.ECacheContentType;
import com.dtstack.flink.sql.side.cache.CacheObj;

/**
 * 仅仅用来标记未命中的维表数据
 * Date: 2018/8/28
 * Company: www.dtstack.com
 * @author xuchao
 */

public class CacheMissVal {

    private static CacheObj missObj = CacheObj.buildCacheObj(ECacheContentType.MissVal, null);

    public static CacheObj getMissKeyObj(){
        return missObj;
    }
}
