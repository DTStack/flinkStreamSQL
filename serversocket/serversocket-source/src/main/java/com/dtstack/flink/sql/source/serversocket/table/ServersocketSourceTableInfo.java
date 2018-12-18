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
package com.dtstack.flink.sql.source.serversocket.table;

import com.dtstack.flink.sql.table.SourceTableInfo;
import org.apache.flink.calcite.shaded.com.google.common.base.Preconditions;

/**
 * Reason:
 * Date: 2018/12/18
 * Company: www.dtstack.com
 *
 * @author maqi
 */
public class ServersocketSourceTableInfo extends SourceTableInfo {
	//version
	private static final String CURR_TYPE = "serversocket";

	public static final String HOSTNAME_KEY = "host";

	public static final String PORT_KEY = "port";

	public static final String DELIMITER_KEY = "delimiter";

	public static final String MAXNUMRETRIES_KEY = "maxNumRetries";


	public ServersocketSourceTableInfo() {
		super.setType(CURR_TYPE);
	}

	private String hostname;

	private int port;

	private String delimiter;

	private long maxNumRetries;

	public String getHostname() {
		return hostname;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getDelimiter() {
		return delimiter;
	}

	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}

	public long getMaxNumRetries() {
		return maxNumRetries;
	}

	public void setMaxNumRetries(long maxNumRetries) {
		this.maxNumRetries = maxNumRetries;
	}


	@Override
	public boolean check() {
		Preconditions.checkNotNull(hostname,"host name not null");
		Preconditions.checkNotNull(port,"port not null");
		Preconditions.checkNotNull(delimiter,"delimiter name not null");
		Preconditions.checkNotNull(maxNumRetries,"maxNumRetries name not null");

		Preconditions.checkArgument(port > 0 && port < 65536, "port is out of range");
		Preconditions.checkArgument(maxNumRetries >= -1, "maxNumRetries must be zero or larger (num retries), or -1 (infinite retries)");
		return false;
	}


}
