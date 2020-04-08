/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dtstack.flink.sql.source.serversocket;

import com.dtstack.flink.sql.source.IStreamSourceGener;
import com.dtstack.flink.sql.source.serversocket.table.ServersocketSourceTableInfo;
import com.dtstack.flink.sql.table.SourceTableInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * Reason:
 * Date: 2018/12/18
 * Company: www.dtstack.com
 *
 * @author maqi
 */
public class ServersocketSource implements IStreamSourceGener<Table> {
	@Override
	public Table genStreamSource(SourceTableInfo sourceTableInfo, StreamExecutionEnvironment env, StreamTableEnvironment tableEnv) {
		ServersocketSourceTableInfo serversocketSourceTableInfo = (ServersocketSourceTableInfo) sourceTableInfo;
		String tableName = serversocketSourceTableInfo.getName();

		TypeInformation[] types = new TypeInformation[serversocketSourceTableInfo.getFields().length];
		for (int i = 0; i < serversocketSourceTableInfo.getFieldClasses().length; i++) {
			types[i] = TypeInformation.of(serversocketSourceTableInfo.getFieldClasses()[i]);
		}
		TypeInformation typeInformation = new RowTypeInfo(types, serversocketSourceTableInfo.getFields());
		String fields = StringUtils.join(serversocketSourceTableInfo.getFields(), ",");

		CustomerSocketTextStreamFunction customerSocketTextStreamFunction = new CustomerSocketTextStreamFunction(serversocketSourceTableInfo, typeInformation,
				serversocketSourceTableInfo.getPhysicalFields(), serversocketSourceTableInfo.getFieldExtraInfoList());

		DataStreamSource serversocketSource = env.addSource(customerSocketTextStreamFunction, tableName, typeInformation);

		return tableEnv.fromDataStream(serversocketSource, fields);
	}
}
