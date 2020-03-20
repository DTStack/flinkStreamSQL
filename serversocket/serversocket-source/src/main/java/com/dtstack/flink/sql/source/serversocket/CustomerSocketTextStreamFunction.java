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

import com.dtstack.flink.sql.format.DeserializationMetricWrapper;
import com.dtstack.flink.sql.format.dtnest.DtNestRowDeserializationSchema;
import com.dtstack.flink.sql.source.serversocket.table.ServersocketSourceTableInfo;
import com.dtstack.flink.sql.table.TableInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.util.Map;


/**
 * Reason:
 * Date: 2018/12/18
 * Company: www.dtstack.com
 *
 * @author maqi
 */
public class CustomerSocketTextStreamFunction implements SourceFunction<Row> {
	private static final Logger LOG = LoggerFactory.getLogger(CustomerSocketTextStreamFunction.class);

	protected DtNestRowDeserializationSchema deserializationSchema;

	protected DeserializationMetricWrapper deserializationMetricWrapper;

	/**
	 * Default delay between successive connection attempts.
	 */
	private static final int DEFAULT_CONNECTION_RETRY_SLEEP = 2000;

	/**
	 * Default connection timeout when connecting to the server socket (infinite).
	 */
	private static final int CONNECTION_TIMEOUT_TIME = 30000;

	private volatile boolean isRunning = true;

	private transient Socket currentSocket;

	private String CHARSET_NAME = "UTF-8";

	ServersocketSourceTableInfo tableInfo;



	public CustomerSocketTextStreamFunction(ServersocketSourceTableInfo tableInfo, TypeInformation<Row> typeInfo,
											Map<String, String> rowAndFieldMapping, List<TableInfo.FieldExtraInfo> fieldExtraInfos) {
		this.tableInfo = tableInfo;
		this.deserializationSchema = new DtNestRowDeserializationSchema(typeInfo, rowAndFieldMapping, fieldExtraInfos, CHARSET_NAME);
		this.deserializationMetricWrapper = new DeserializationMetricWrapper(typeInfo, deserializationSchema);
	}

	@Override
	public void run(SourceContext<Row> ctx) throws Exception {
		final StringBuilder buffer = new StringBuilder();
		long attempt = 0;

		while (isRunning) {
			try (Socket socket = new Socket()) {
				currentSocket = socket;
				LOG.info("Connecting to server socket " + tableInfo.getHostname() + ':' + tableInfo.getPort());
				socket.connect(new InetSocketAddress(tableInfo.getHostname(), tableInfo.getPort()), CONNECTION_TIMEOUT_TIME);

				try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {
					char[] cbuf = new char[8192];
					int bytesRead;
					while (isRunning && (bytesRead = reader.read(cbuf)) != -1) {
						buffer.append(cbuf, 0, bytesRead);
						int delimPos;
						String delimiter = tableInfo.getDelimiter();
						while (buffer.length() >= delimiter.length() && (delimPos = buffer.indexOf(delimiter)) != -1) {
							String record = buffer.substring(0, delimPos);
							// truncate trailing carriage return
							if (delimiter.equals("\n") && record.endsWith("\r")) {
								record = record.substring(0, record.length() - 1);
							}
							try {
								Row row = deserializationMetricWrapper.deserialize(record.getBytes());
								ctx.collect(row);
							} catch (Exception e) {
								LOG.error("parseData error ", e);
							} finally {
								buffer.delete(0, delimPos + delimiter.length());
							}
						}
					}
				}
			}

			// if we dropped out of this loop due to an EOF, sleep and retry
			if (isRunning) {
				attempt++;
				if (tableInfo.getMaxNumRetries() == -1 || attempt < tableInfo.getMaxNumRetries()) {
					Thread.sleep(DEFAULT_CONNECTION_RETRY_SLEEP);
				} else {
					// this should probably be here, but some examples expect simple exists of the stream source
					// throw new EOFException("Reached end of stream and reconnects are not enabled.");
					break;
				}
			}
		}

		// collect trailing data
		if (buffer.length() > 0) {
			try {
				Row row = deserializationMetricWrapper.deserialize(buffer.toString().getBytes());
				ctx.collect(row);
			} catch (Exception e) {
				LOG.error("parseData error ", e);
			}
		}
	}

	@Override
	public void cancel() {
		isRunning = false;

		// we need to close the socket as well, because the Thread.interrupt() function will
		// not wake the thread in the socketStream.read() method when blocked.
		Socket theSocket = this.currentSocket;
		if (theSocket != null) {
			IOUtils.closeSocket(theSocket);
		}
	}

}
