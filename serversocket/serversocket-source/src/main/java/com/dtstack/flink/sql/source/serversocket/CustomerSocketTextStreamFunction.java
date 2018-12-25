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

import com.dtstack.flink.sql.source.serversocket.table.ServersocketSourceTableInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Iterator;


/**
 * Reason:
 * Date: 2018/12/18
 * Company: www.dtstack.com
 *
 * @author maqi
 */
public class CustomerSocketTextStreamFunction implements SourceFunction<Row> {
	private static final Logger LOG = LoggerFactory.getLogger(CustomerSocketTextStreamFunction.class);

	/**
	 * Default delay between successive connection attempts.
	 */
	private static final int DEFAULT_CONNECTION_RETRY_SLEEP = 2000;

	/**
	 * Default connection timeout when connecting to the server socket (infinite).
	 */
	private static final int CONNECTION_TIMEOUT_TIME = 0;

	private final ObjectMapper objectMapper = new ObjectMapper();

	/**
	 * Type information describing the result type.
	 */
	private final TypeInformation<Row> typeInfo;

	/**
	 * Field names to parse. Indices match fieldTypes indices.
	 */
	private final String[] fieldNames;

	/**
	 * Types to parse fields as. Indices match fieldNames indices.
	 */
	private final TypeInformation<?>[] fieldTypes;

	private volatile boolean isRunning = true;

	private transient Socket currentSocket;

	ServersocketSourceTableInfo tableInfo;

	public CustomerSocketTextStreamFunction(ServersocketSourceTableInfo tableInfo, TypeInformation<Row> typeInfo) {
		this.typeInfo = typeInfo;

		this.fieldNames = ((RowTypeInfo) typeInfo).getFieldNames();

		this.fieldTypes = ((RowTypeInfo) typeInfo).getFieldTypes();

		this.tableInfo = tableInfo;
	}

	@Override
	public void run(SourceContext<Row> ctx) throws Exception {
		final StringBuilder buffer = new StringBuilder();
		long attempt = 0;

		while (isRunning) {
			try {
				Socket socket = new Socket();
				currentSocket = socket;
				socket.connect(new InetSocketAddress(tableInfo.getHostname(), tableInfo.getPort()), CONNECTION_TIMEOUT_TIME);

				BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
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
						ctx.collect(convertToRow(record));
						buffer.delete(0, delimPos + delimiter.length());
					}
				}
			} catch (Exception e) {
				LOG.info("Connection server failed, Please check configuration  !!!!!!!!!!!!!!!!");
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
			ctx.collect(convertToRow(buffer.toString()));
		}
	}

	public Row convertToRow(String record) throws IOException {
		JsonNode root = objectMapper.readTree(record);
		Row row = new Row(fieldNames.length);
		for (int i = 0; i < fieldNames.length; i++) {
			JsonNode node = getIgnoreCase(root, fieldNames[i]);
			if (node == null) {
				row.setField(i, null);
			} else {
				// Read the value as specified type
				Object value = objectMapper.treeToValue(node, fieldTypes[i].getTypeClass());
				row.setField(i, value);
			}
		}
		return row;
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

	public JsonNode getIgnoreCase(JsonNode jsonNode, String key) {
		Iterator<String> iter = jsonNode.fieldNames();
		while (iter.hasNext()) {
			String key1 = iter.next();
			if (key1.equalsIgnoreCase(key)) {
				return jsonNode.get(key1);
			}
		}
		return null;
	}
}
